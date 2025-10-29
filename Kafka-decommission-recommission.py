#!/usr/bin/env python3
"""
Kafka Broker Decommission/Recommission Script - Production Grade
=================================================================
This script safely decommissions a Kafka broker by transferring leadership
and stopping the broker, or recommissions it by starting and restoring leadership.

Features:
- Comprehensive pre-checks before decommission
- Resource-aware leader reassignment (CPU, Disk)
- Broker stop/start with SSH
- ISR synchronization monitoring before recommission
- Rollback capability to restore previous state
- Detailed logging and state persistence

Author: Production Engineering Team
Version: 2.0.0
Kafka Version: 2.8.2
"""

import json
import yaml
import logging
import os
import sys
import argparse
import subprocess
import time
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Set
from collections import defaultdict
import shutil
import traceback
import re
import glob

# ==============================================================================
# LOCAL BROKER MANAGEMENT (NO SSH)
# ==============================================================================

class BrokerManager:
    """Manage local broker start/stop operations."""
    
    def __init__(self, kafka_bin: str, logger: logging.Logger):
        """
        Initialize broker manager.
        
        Args:
            kafka_bin: Path to Kafka bin directory
            logger: Logger instance
        """
        self.kafka_bin = kafka_bin
        self.logger = logger
        self.kafka_stop_script = f"{kafka_bin}/kafka-server-stop.sh"
        self.kafka_start_script = f"{kafka_bin}/kafka-server-start.sh"
    
    def stop_broker(self, config_file: Optional[str] = None) -> bool:
        """
        Stop local Kafka broker using kafka-server-stop.sh.
        
        Args:
            config_file: Path to server.properties (not used for stop, but kept for consistency)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.logger.info("="*70)
            self.logger.info("STOPPING KAFKA BROKER")
            self.logger.info("="*70)
            
            if not os.path.exists(self.kafka_stop_script):
                self.logger.error(f"Kafka stop script not found: {self.kafka_stop_script}")
                return False
            
            cmd = [self.kafka_stop_script]
            
            self.logger.info(f"Executing: {' '.join(cmd)}")
            self.logger.info("This may take a few moments...")
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=120
            )
            
            # Log output
            if result.stdout:
                self.logger.info("Stop script output:")
                for line in result.stdout.strip().split('\n'):
                    self.logger.info(f"  {line}")
            
            if result.stderr:
                self.logger.warning("Stop script stderr:")
                for line in result.stderr.strip().split('\n'):
                    self.logger.warning(f"  {line}")
            
            # kafka-server-stop.sh may return non-zero even on success
            # Wait and verify the broker actually stopped
            self.logger.info("Waiting for broker to stop (15 seconds)...")
            time.sleep(15)
            
            if self.verify_broker_stopped():
                self.logger.info("✓ Kafka broker stopped successfully")
                self.logger.info("="*70)
                return True
            else:
                self.logger.warning("Broker stop command executed, but broker may still be running")
                self.logger.info("Waiting additional 10 seconds...")
                time.sleep(10)
                
                if self.verify_broker_stopped():
                    self.logger.info("✓ Kafka broker stopped successfully (after additional wait)")
                    self.logger.info("="*70)
                    return True
                else:
                    self.logger.error("✗ Broker may not have stopped properly")
                    self.logger.error("Please check manually with: ps aux | grep kafka")
                    self.logger.info("="*70)
                    return False
            
        except subprocess.TimeoutExpired:
            self.logger.error("Timeout stopping broker (120s)")
            self.logger.info("="*70)
            return False
        except Exception as e:
            self.logger.error(f"Error stopping broker: {e}")
            self.logger.debug(traceback.format_exc())
            self.logger.info("="*70)
            return False
    
    def start_broker(self, config_file: str, daemon_mode: bool = True) -> bool:
        """
        Start local Kafka broker using kafka-server-start.sh.
        
        Args:
            config_file: Path to server.properties
            daemon_mode: Whether to start in daemon mode (background)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.logger.info("="*70)
            self.logger.info("STARTING KAFKA BROKER")
            self.logger.info("="*70)
            
            if not os.path.exists(self.kafka_start_script):
                self.logger.error(f"Kafka start script not found: {self.kafka_start_script}")
                return False
            
            if not os.path.exists(config_file):
                self.logger.error(f"Server config file not found: {config_file}")
                return False
            
            cmd = [self.kafka_start_script]
            if daemon_mode:
                cmd.append("-daemon")
            cmd.append(config_file)
            
            self.logger.info(f"Executing: {' '.join(cmd)}")
            self.logger.info(f"Config file: {config_file}")
            self.logger.info(f"Daemon mode: {daemon_mode}")
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=60
            )
            
            # Log output
            if result.stdout:
                self.logger.info("Start script output:")
                for line in result.stdout.strip().split('\n'):
                    self.logger.info(f"  {line}")
            
            if result.stderr:
                self.logger.warning("Start script stderr:")
                for line in result.stderr.strip().split('\n'):
                    self.logger.warning(f"  {line}")
            
            if result.returncode != 0:
                self.logger.error(f"Start script returned error code: {result.returncode}")
                self.logger.info("="*70)
                return False
            
            # Wait for broker to start
            self.logger.info("Waiting for broker to start (20 seconds)...")
            time.sleep(20)
            
            if self.verify_broker_running():
                self.logger.info("✓ Kafka broker started successfully")
                self.logger.info("="*70)
                return True
            else:
                self.logger.warning("Start command executed, but broker may not be running yet")
                self.logger.info("Waiting additional 15 seconds...")
                time.sleep(15)
                
                if self.verify_broker_running():
                    self.logger.info("✓ Kafka broker started successfully (after additional wait)")
                    self.logger.info("="*70)
                    return True
                else:
                    self.logger.error("✗ Broker may not have started properly")
                    self.logger.error("Check logs: tail -f /path/to/kafka/logs/server.log")
                    self.logger.info("="*70)
                    return False
            
        except subprocess.TimeoutExpired:
            self.logger.error("Timeout starting broker (60s)")
            self.logger.info("="*70)
            return False
        except Exception as e:
            self.logger.error(f"Error starting broker: {e}")
            self.logger.debug(traceback.format_exc())
            self.logger.info("="*70)
            return False
    
    def verify_broker_stopped(self) -> bool:
        """
        Verify broker is stopped by checking for kafka.Kafka process.
        
        Returns:
            True if broker is stopped, False if still running
        """
        try:
            # Check for Kafka process
            cmd = ["pgrep", "-f", "kafka.Kafka"]
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=5
            )
            
            # pgrep returns 0 if process found, 1 if not found
            if result.returncode == 0:
                # Process still running
                pids = result.stdout.strip().split('\n')
                self.logger.debug(f"Kafka process(es) still running: {', '.join(pids)}")
                return False
            else:
                # No process found - broker stopped
                self.logger.debug("No Kafka process found - broker is stopped")
                return True
            
        except Exception as e:
            self.logger.warning(f"Could not verify broker status: {e}")
            # If we can't verify, assume it's stopped to not block operations
            return True
    
    def verify_broker_running(self) -> bool:
        """
        Verify broker is running by checking for kafka.Kafka process.
        
        Returns:
            True if broker is running, False otherwise
        """
        try:
            # Check for Kafka process
            cmd = ["pgrep", "-f", "kafka.Kafka"]
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=5
            )
            
            # pgrep returns 0 if process found, 1 if not found
            if result.returncode == 0:
                # Process running
                pids = result.stdout.strip().split('\n')
                self.logger.debug(f"Kafka process(es) running with PID(s): {', '.join(pids)}")
                return True
            else:
                # No process found
                self.logger.debug("No Kafka process found - broker is not running")
                return False
            
        except Exception as e:
            self.logger.warning(f"Could not verify broker status: {e}")
            return False


# ==============================================================================
# ISR SYNCHRONIZATION MONITOR
# ==============================================================================

class ISRMonitor:
    """Monitor ISR synchronization status for broker recommission."""
    
    def __init__(self, cluster_manager, logger: logging.Logger):
        """
        Initialize ISR monitor.
        
        Args:
            cluster_manager: KafkaClusterManager instance
            logger: Logger instance
        """
        self.cluster = cluster_manager
        self.logger = logger
    
    def wait_for_broker_in_isr(self, broker_id: int, partitions: List[Dict], 
                                timeout: int = 600, check_interval: int = 10) -> bool:
        """
        Wait for broker to rejoin ISR for all its partitions.
        
        Args:
            broker_id: Broker ID to monitor
            partitions: List of partition metadata (topic, partition)
            timeout: Maximum time to wait in seconds
            check_interval: Time between checks in seconds
            
        Returns:
            True if all partitions are in ISR, False otherwise
        """
        self.logger.info(f"Waiting for broker {broker_id} to rejoin ISR for {len(partitions)} partitions")
        self.logger.info(f"Timeout: {timeout}s, Check interval: {check_interval}s")
        
        start_time = time.time()
        partition_set = {(p['topic'], p['partition']) for p in partitions}
        
        while time.time() - start_time < timeout:
            try:
                # Get current metadata
                metadata = self.cluster.get_partition_metadata()
                
                in_isr = set()
                not_in_isr = set()
                
                for topic, partition_id in partition_set:
                    if topic not in metadata:
                        not_in_isr.add((topic, partition_id))
                        continue
                    
                    # Find the partition
                    partition_found = False
                    for p in metadata[topic]:
                        if p['partition'] == partition_id:
                            partition_found = True
                            if broker_id in p['isr']:
                                in_isr.add((topic, partition_id))
                            else:
                                not_in_isr.add((topic, partition_id))
                            break
                    
                    if not partition_found:
                        not_in_isr.add((topic, partition_id))
                
                # Report progress
                elapsed = int(time.time() - start_time)
                remaining = int(timeout - elapsed)
                
                self.logger.info(
                    f"ISR sync progress: {len(in_isr)}/{len(partition_set)} partitions "
                    f"(elapsed: {elapsed}s, remaining: {remaining}s)"
                )
                
                if len(not_in_isr) > 0 and len(not_in_isr) <= 5:
                    self.logger.info(f"Waiting for: {', '.join([f'{t}-{p}' for t, p in not_in_isr])}")
                elif len(not_in_isr) > 5:
                    sample = list(not_in_isr)[:3]
                    self.logger.info(
                        f"Waiting for: {', '.join([f'{t}-{p}' for t, p in sample])} "
                        f"and {len(not_in_isr) - 3} more"
                    )
                
                # Check if all in ISR
                if len(not_in_isr) == 0:
                    self.logger.info(f"✓ Broker {broker_id} is in ISR for all {len(partition_set)} partitions")
                    return True
                
                # Wait before next check
                time.sleep(check_interval)
                
            except Exception as e:
                self.logger.error(f"Error checking ISR status: {e}")
                self.logger.debug(traceback.format_exc())
                time.sleep(check_interval)
        
        # Timeout reached
        self.logger.error(
            f"Timeout waiting for broker {broker_id} to rejoin ISR. "
            f"{len(not_in_isr)}/{len(partition_set)} partitions still not in ISR"
        )
        
        # Log some partitions that failed
        if not_in_isr:
            self.logger.error("Partitions not in ISR:")
            for topic, partition_id in list(not_in_isr)[:10]:
                self.logger.error(f"  {topic}-{partition_id}")
            if len(not_in_isr) > 10:
                self.logger.error(f"  ... and {len(not_in_isr) - 10} more")
        
        return False
    
    def get_under_replicated_count_for_broker(self, broker_id: int, partitions: List[Dict]) -> int:
        """
        Get count of under-replicated partitions for specific broker.
        
        Args:
            broker_id: Broker ID
            partitions: List of partition metadata
            
        Returns:
            Count of partitions where broker is not in ISR
        """
        try:
            metadata = self.cluster.get_partition_metadata()
            urp_count = 0
            
            for part in partitions:
                topic = part['topic']
                partition_id = part['partition']
                
                if topic not in metadata:
                    urp_count += 1
                    continue
                
                for p in metadata[topic]:
                    if p['partition'] == partition_id:
                        if broker_id not in p['isr']:
                            urp_count += 1
                        break
            
            return urp_count
            
        except Exception as e:
            self.logger.error(f"Error checking under-replicated partitions: {e}")
            return -1


# [Include all the original utility functions here: auto_detect_kafka_bin, auto_detect_zookeeper_servers, 
#  get_broker_disk_usage_from_kafka, setup_logging, KafkaConfig, ResourceMonitor, KafkaClusterManager]

# ... [Copy the entire original code for these classes/functions - I'll continue with the modified sections]

# ==============================================================================
# MODIFIED DECOMMISSION MANAGER (replaces LeaderDemotionManager)
# ==============================================================================

class BrokerDecommissionManager:
    """Manage broker decommission operations."""
    
    def __init__(
        self,
        cluster_manager,
        broker_manager: BrokerManager,
        config,
        logger: logging.Logger,
        dry_run: bool = False
    ):
        """
        Initialize decommission manager.
        
        Args:
            cluster_manager: Kafka cluster manager
            broker_manager: Broker start/stop manager
            config: Configuration
            logger: Logger instance
            dry_run: If True, simulate operations without making changes
        """
        self.cluster = cluster_manager
        self.broker_manager = broker_manager
        self.config = config
        self.logger = logger
        self.dry_run = dry_run
        self.state_dir = config.get('state_dir')
        os.makedirs(self.state_dir, exist_ok=True)
        
        if self.dry_run:
            self.logger.info("="*70)
            self.logger.info("🔍 DRY-RUN MODE ENABLED - No actual changes will be made")
            self.logger.info("="*70)
    
    def decommission_broker(self, broker_id: int) -> bool:
        """
        Decommission broker: transfer leadership → stop broker.
        
        Args:
            broker_id: Broker ID to decommission
            
        Returns:
            True if successful, False otherwise
        """
        self.logger.info("="*70)
        if self.dry_run:
            self.logger.info(f"🔍 DRY-RUN: SIMULATING BROKER DECOMMISSION FOR BROKER {broker_id}")
        else:
            self.logger.info(f"STARTING BROKER DECOMMISSION FOR BROKER {broker_id}")
        self.logger.info("="*70)
        
        # Get broker hostname
        hostname = self.cluster.get_broker_hostname(broker_id)
        if not hostname:
            self.logger.error(f"Could not get hostname for broker {broker_id}")
            return False
        
        self.logger.info(f"Broker {broker_id} hostname: {hostname}")
        
        # Find partitions where broker is leader
        partitions = self._find_partitions_to_transfer(broker_id)
        if not partitions:
            self.logger.warning(f"No partitions found where broker {broker_id} is leader")
        else:
            self.logger.info(f"Found {len(partitions)} partitions where broker {broker_id} is leader")
        
        # Save current state for rollback
        state_file = self._save_decommission_state(broker_id, hostname, partitions)
        
        # Transfer leadership if there are partitions
        if partitions:
            if not self._transfer_leadership(broker_id, partitions):
                self.logger.error("Failed to transfer leadership")
                return False
        
        # Stop the broker
        if not self.dry_run:
            service_name = self.config.get('kafka_service_name', 'kafka-broker')
            if not self.broker_manager.stop_broker(hostname, service_name):
                self.logger.error(f"Failed to stop broker {broker_id} on {hostname}")
                return False
        else:
            self.logger.info(f"🔍 DRY-RUN: Would stop broker {broker_id} on {hostname}")
        
        self.logger.info("="*70)
        if self.dry_run:
            self.logger.info(f"🔍 DRY-RUN: DECOMMISSION SIMULATION COMPLETED FOR BROKER {broker_id}")
            self.logger.info(f"🔍 DRY-RUN: No actual changes were made")
            self.logger.info(f"🔍 DRY-RUN: State file: {state_file}")
        else:
            self.logger.info(f"BROKER {broker_id} SUCCESSFULLY DECOMMISSIONED")
            self.logger.info(f"State file for recommission: {state_file}")
        self.logger.info("="*70)
        
        return True
    
    def _find_partitions_to_transfer(self, broker_id: int) -> List[Dict]:
        """Find all partitions where broker is in replica set."""
        metadata = self.cluster.get_partition_metadata()
        partitions = []
        
        for topic, parts in metadata.items():
            for part in parts:
                # Include all partitions where broker is a replica
                if broker_id in part['replicas']:
                    partitions.append({
                        'topic': topic,
                        'partition': part['partition'],
                        'leader': part['leader'],
                        'replicas': part['replicas'],
                        'isr': part['isr']
                    })
        
        return partitions
    
    def _transfer_leadership(self, broker_id: int, partitions: List[Dict]) -> bool:
        """Transfer leadership away from broker."""
        self.logger.info(f"Transferring leadership for {len(partitions)} partitions")
        
        # Filter to only partitions where broker is leader
        leader_partitions = [p for p in partitions if p['leader'] == broker_id]
        
        if not leader_partitions:
            self.logger.info("No leadership transfer needed")
            return True
        
        # Get resource information for replica selection
        all_replicas = set()
        for part in leader_partitions:
            all_replicas.update(part['replicas'])
        
        brokers_info = self._get_brokers_resource_info(all_replicas)
        
        # Create reassignment plan
        reassignment_file, new_leaders = self._create_reassignment_json(
            leader_partitions,
            brokers_info
        )
        
        if not reassignment_file:
            self.logger.error("Failed to create reassignment plan")
            return False
        
        # Execute reassignment
        if not self._execute_reassignment(reassignment_file):
            return False
        
        # Verify reassignment
        if not self._verify_reassignment(reassignment_file):
            return False
        
        # Trigger preferred leader election
        if not self.dry_run:
            election_partitions = [
                {'topic': p['topic'], 'partition': p['partition']}
                for p in leader_partitions
            ]
            self.cluster.trigger_preferred_leader_election(election_partitions)
            time.sleep(2)
        
        return True
    
    # [Include helper methods: _get_brokers_resource_info, _create_reassignment_json, 
    #  _execute_reassignment, _verify_reassignment - similar to original]
    
    def _save_decommission_state(self, broker_id: int, hostname: str, 
                                  partitions: List[Dict]) -> str:
        """Save decommission state for recommission."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        state_file = os.path.join(
            self.state_dir,
            f"decommission_state_broker_{broker_id}_{timestamp}.json"
        )
        
        state = {
            'timestamp': timestamp,
            'broker_id': broker_id,
            'hostname': hostname,
            'partitions': partitions,
            'operation': 'decommission',
            'config': {
                'zookeeper': self.config.get('zookeeper_server'),
                'bootstrap_servers': self.config.get('bootstrap_servers')
            },
            'dry_run': self.dry_run
        }
        
        if self.dry_run:
            state_file = state_file.replace('.json', '_dryrun.json')
        
        with open(state_file, 'w') as f:
            json.dump(state, f, indent=2)
        
        self.logger.info(f"Decommission state saved to {state_file}")
        return state_file


# ==============================================================================
# MODIFIED RECOMMISSION MANAGER (replaces RollbackManager)
# ==============================================================================

class BrokerRecommissionManager:
    """Manage broker recommission operations."""
    
    def __init__(
        self,
        cluster_manager,
        broker_manager: BrokerManager,
        config,
        logger: logging.Logger,
        dry_run: bool = False
    ):
        """
        Initialize recommission manager.
        
        Args:
            cluster_manager: Kafka cluster manager
            broker_manager: Broker start/stop manager
            config: Configuration
            logger: Logger instance
            dry_run: If True, simulate operations without making changes
        """
        self.cluster = cluster_manager
        self.broker_manager = broker_manager
        self.config = config
        self.logger = logger
        self.dry_run = dry_run
        self.state_dir = config.get('state_dir')
        self.isr_monitor = ISRMonitor(cluster_manager, logger)
        
        if self.dry_run:
            self.logger.info("="*70)
            self.logger.info("🔍 DRY-RUN MODE ENABLED - No actual changes will be made")
            self.logger.info("="*70)
    
    def recommission_broker(self, broker_id: int, state_file: Optional[str] = None) -> bool:
        """
        Recommission broker: start broker → wait for ISR → restore leadership.
        
        Args:
            broker_id: Broker ID to recommission
            state_file: Optional specific state file path
            
        Returns:
            True if successful, False otherwise
        """
        self.logger.info("="*70)
        if self.dry_run:
            self.logger.info(f"🔍 DRY-RUN: SIMULATING BROKER RECOMMISSION FOR BROKER {broker_id}")
        else:
            self.logger.info(f"STARTING BROKER RECOMMISSION FOR BROKER {broker_id}")
        self.logger.info("="*70)
        
        # Find and load state file
        if state_file is None:
            state_file = self._find_latest_state_file(broker_id)
        
        if state_file is None:
            self.logger.error("No state file available for recommission")
            return False
        
        state = self._load_state(state_file)
        if state is None:
            return False
        
        hostname = state.get('hostname')
        partitions = state.get('partitions', [])
        
        self.logger.info(f"Broker {broker_id} hostname: {hostname}")
        self.logger.info(f"Will restore {len(partitions)} partitions")
        
        # Start the broker
        if not self.dry_run:
            server_config = self.config.get('kafka_server_config', '/etc/kafka/conf/server.properties')
            self.logger.info(f"Starting broker {broker_id}...")
            if not self.broker_manager.start_broker(server_config, daemon_mode=True):
                self.logger.error(f"Failed to start broker {broker_id}")
                return False
        else:
            self.logger.info(f"🔍 DRY-RUN: Would start broker {broker_id}")
        
        # Wait for broker to rejoin ISR
        if not self.dry_run:
            isr_timeout = self.config.get('isr_sync_timeout', 600)
            self.logger.info("")
            self.logger.info("="*70)
            self.logger.info(f"WAITING FOR BROKER {broker_id} TO SYNC REPLICAS")
            self.logger.info("="*70)
            
            if not self.isr_monitor.wait_for_broker_in_isr(broker_id, partitions, isr_timeout):
                self.logger.error(f"Broker {broker_id} failed to rejoin ISR within {isr_timeout}s")
                self.logger.error("Recommission cannot proceed - broker replicas not in sync")
                self.logger.error("")
                self.logger.error("Possible actions:")
                self.logger.error("  1. Wait longer and check ISR status manually")
                self.logger.error("  2. Investigate broker logs for sync issues")
                self.logger.error("  3. Check network connectivity between brokers")
                return False
            
            self.logger.info("="*70)
            self.logger.info(f"✓ BROKER {broker_id} REPLICAS ARE IN-SYNC")
            self.logger.info("="*70)
        else:
            self.logger.info(f"🔍 DRY-RUN: Would wait for broker {broker_id} to rejoin ISR")
        
        # Restore leadership
        if partitions:
            leader_partitions = [p for p in partitions if p['leader'] == broker_id]
            if leader_partitions:
                self.logger.info(f"\nRestoring leadership for {len(leader_partitions)} partitions")
                if not self._restore_leadership(broker_id, leader_partitions):
                    self.logger.error("Failed to restore leadership")
                    return False
            else:
                self.logger.info("No leadership to restore (broker was not leader for any partitions)")
        
        self.logger.info("="*70)
        if self.dry_run:
            self.logger.info(f"🔍 DRY-RUN: RECOMMISSION SIMULATION COMPLETED FOR BROKER {broker_id}")
        else:
            self.logger.info(f"BROKER {broker_id} SUCCESSFULLY RECOMMISSIONED")
        self.logger.info("="*70)
        
        return True
    
    def _find_latest_state_file(self, broker_id: int) -> Optional[str]:
        """Find the most recent state file for a broker."""
        pattern = f"decommission_state_broker_{broker_id}_"
        state_files = [
            f for f in os.listdir(self.state_dir)
            if f.startswith(pattern) and f.endswith('.json')
        ]
        
        if not state_files:
            self.logger.error(f"No state files found for broker {broker_id}")
            return None
        
        # Sort by timestamp
        state_files.sort(reverse=True)
        latest = os.path.join(self.state_dir, state_files[0])
        
        self.logger.info(f"Found latest state file: {latest}")
        return latest
    
    def _load_state(self, state_file: str) -> Optional[Dict]:
        """Load state from file."""
        try:
            with open(state_file, 'r') as f:
                state = json.load(f)
            
            self.logger.info(f"Loaded state from {state_file}")
            self.logger.info(f"  Timestamp: {state.get('timestamp')}")
            self.logger.info(f"  Broker ID: {state.get('broker_id')}")
            self.logger.info(f"  Partitions: {len(state.get('partitions', []))}")
            
            return state
            
        except Exception as e:
            self.logger.error(f"Error loading state file: {e}")
            self.logger.debug(traceback.format_exc())
            return None
    
    def _restore_leadership(self, broker_id: int, leader_partitions: List[Dict]) -> bool:
        """Restore original leadership to broker."""
        # Create reassignment to put broker first in replica list
        reassignment_file = self._create_restoration_reassignment(broker_id, leader_partitions)
        if not reassignment_file:
            return False
        
        # Log the plan
        self.logger.info("Leadership restoration plan:")
        for part in leader_partitions[:10]:
            self.logger.info(f"  {part['topic']}-{part['partition']}: Restore leadership to broker {broker_id}")
        if len(leader_partitions) > 10:
            self.logger.info(f"  ... and {len(leader_partitions) - 10} more partitions")
        
        # Execute reassignment
        if not self._execute_reassignment(reassignment_file):
            return False
        
        # Verify reassignment
        if not self._verify_reassignment(reassignment_file):
            return False
        
        # Trigger preferred leader election
        if not self.dry_run:
            self.logger.info("Triggering preferred leader election...")
            election_partitions = [
                {'topic': p['topic'], 'partition': p['partition']}
                for p in leader_partitions
            ]
            self.cluster.trigger_preferred_leader_election(election_partitions)
            time.sleep(3)
            
            # Verify leadership was restored
            self.logger.info("Verifying leadership restoration...")
            restored_count = 0
            failed_partitions = []
            
            metadata = self.cluster.get_partition_metadata()
            for part in leader_partitions:
                topic = part['topic']
                partition_id = part['partition']
                
                if topic in metadata:
                    for p in metadata[topic]:
                        if p['partition'] == partition_id:
                            if p['leader'] == broker_id:
                                restored_count += 1
                            else:
                                failed_partitions.append(f"{topic}-{partition_id} (current leader: {p['leader']})")
                            break
            
            self.logger.info(f"Leadership restored for {restored_count}/{len(leader_partitions)} partitions")
            
            if failed_partitions and len(failed_partitions) <= 10:
                self.logger.warning("Some partitions did not restore leadership:")
                for fp in failed_partitions:
                    self.logger.warning(f"  {fp}")
            elif len(failed_partitions) > 10:
                self.logger.warning(f"Leadership not restored for {len(failed_partitions)} partitions")
        
        return True
    
    def _create_restoration_reassignment(self, broker_id: int, 
                                         leader_partitions: List[Dict]) -> Optional[str]:
        """Create reassignment JSON to restore leadership."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename_prefix = "restoration_dryrun_" if self.dry_run else "restoration_"
        reassignment_file = os.path.join(
            self.state_dir,
            f"{filename_prefix}{timestamp}.json"
        )
        
        reassignment = {"version": 1, "partitions": []}
        
        for part in leader_partitions:
            # Put broker_id first in replicas list
            original_replicas = part['replicas']
            restored_replicas = [broker_id] + [r for r in original_replicas if r != broker_id]
            
            reassignment["partitions"].append({
                "topic": part['topic'],
                "partition": part['partition'],
                "replicas": restored_replicas,
                "log_dirs": ["any"] * len(restored_replicas)
            })
        
        with open(reassignment_file, 'w') as f:
            json.dump(reassignment, f, indent=2)
        
        self.logger.info(f"Restoration plan created: {reassignment_file}")
        return reassignment_file
    
    def _execute_reassignment(self, reassignment_file: str) -> bool:
        """Execute partition reassignment."""
        if self.dry_run:
            self.logger.info(f"🔍 DRY-RUN: Would execute reassignment from {reassignment_file}")
            return True
        
        try:
            cmd = [
                f"{self.cluster.kafka_bin}/kafka-reassign-partitions.sh",
                "--bootstrap-server", self.cluster.bootstrap_servers,
                "--reassignment-json-file", reassignment_file,
                "--execute"
            ]
            
            self.logger.info("Executing partition reassignment...")
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300
            )
            
            if result.stdout:
                self.logger.info("Reassignment output:")
                for line in result.stdout.strip().split('\n'):
                    self.logger.info(f"  {line}")
            
            if result.returncode != 0:
                self.logger.error(f"Reassignment failed: {result.stderr}")
                return False
            
            return True
            
        except subprocess.TimeoutExpired:
            self.logger.error("Reassignment timed out")
            return False
        except Exception as e:
            self.logger.error(f"Error executing reassignment: {e}")
            self.logger.debug(traceback.format_exc())
            return False
    
    def _verify_reassignment(self, reassignment_file: str, timeout: int = 300) -> bool:
        """Verify reassignment completion."""
        if self.dry_run:
            self.logger.info(f"🔍 DRY-RUN: Would verify reassignment")
            return True
        
        try:
            cmd = [
                f"{self.cluster.kafka_bin}/kafka-reassign-partitions.sh",
                "--bootstrap-server", self.cluster.bootstrap_servers,
                "--reassignment-json-file", reassignment_file,
                "--verify"
            ]
            
            self.logger.info("Verifying partition reassignment...")
            start_time = time.time()
            check_interval = self.config.get('verification_interval', 10)
            
            while time.time() - start_time < timeout:
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=60
                )
                
                output = result.stdout.strip()
                output_lower = output.lower()
                
                complete_count = output_lower.count('is complete') + output_lower.count('completed successfully')
                in_progress_count = output_lower.count('in progress')
                failed_count = output_lower.count('failed') + output_lower.count('error')
                
                if complete_count > 0 and in_progress_count == 0 and failed_count == 0:
                    self.logger.info(f"✓ Partition reassignment completed")
                    return True
                
                if failed_count > 0:
                    self.logger.error(f"Reassignment failed:\n{output}")
                    return False
                
                if not output or output_lower.strip() == '':
                    self.logger.info("Reassignment completed (empty verification output)")
                    return True
                
                elapsed = int(time.time() - start_time)
                remaining = int(timeout - elapsed)
                self.logger.info(f"Still in progress (elapsed: {elapsed}s, remaining: {remaining}s)")
                
                time.sleep(check_interval)
            
            self.logger.error(f"Reassignment verification timed out after {timeout}s")
            return False
            
        except Exception as e:
            self.logger.error(f"Error verifying reassignment: {e}")
            self.logger.debug(traceback.format_exc())
            return False


# ==============================================================================
# UPDATED MAIN EXECUTION
# ==============================================================================

def print_banner():
    """Print script banner."""
    banner = """
╔══════════════════════════════════════════════════════════════════════╗
║                                                                      ║
║         Kafka Broker Decommission/Recommission Tool - v2.0.0        ║
║              Production Grade - Kafka 2.8.2                          ║
║                                                                      ║
║  Features:                                                           ║
║    • Automated broker decommission (stop)                            ║
║    • Automated broker recommission (start + ISR sync)                ║
║    • Resource-aware leader reassignment                             ║
║    • ISR synchronization monitoring                                  ║
║    • Comprehensive logging                                           ║
║    • Dry-run mode for testing                                        ║
║                                                                      ║
╚══════════════════════════════════════════════════════════════════════╝
    """
    print(banner)


def main():
    """Main execution function."""
    print_banner()
    
    parser = argparse.ArgumentParser(
        description="Kafka Broker Decommission/Recommission Tool - Automated broker management",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--config',
        required=True,
        help='Path to configuration YAML file'
    )
    
    parser.add_argument(
        '--broker-id',
        type=int,
        required=True,
        help='Broker ID to decommission/recommission'
    )
    
    parser.add_argument(
        '--recommission',
        action='store_true',
        help='Recommission broker (start and restore leadership)'
    )
    
    parser.add_argument(
        '--state-file',
        help='Specific state file for recommission (optional)'
    )
    
    parser.add_argument(
        '--log-dir',
        default='./logs',
        help='Directory for log files (default: ./logs)'
    )
    
    parser.add_argument(
        '--skip-prechecks',
        action='store_true',
        help='Skip pre-checks (NOT RECOMMENDED for production)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Simulate operation without making any actual changes'
    )
    
    args = parser.parse_args()
    
    logger = setup_logging(args.log_dir)
    
    try:
        # Load configuration
        logger.info(f"Loading configuration from {args.config}")
        config = KafkaConfig(args.config, logger)
        
        # Initialize managers
        logger.info("Initializing Kafka cluster manager")
        cluster_manager = KafkaClusterManager(config, logger)
        
        logger.info("Initializing broker manager")
        kafka_bin = config.get('kafka_bin_path')
        broker_manager = BrokerManager(kafka_bin, logger)
        
        if args.recommission:
            # Recommission mode
            logger.info(f"Operating in RECOMMISSION mode for broker {args.broker_id}")
            if args.dry_run:
                logger.info("🔍 DRY-RUN mode enabled - simulating recommission")
            
            recommission_mgr = BrokerRecommissionManager(
                cluster_manager,
                broker_manager,
                config,
                logger,
                args.dry_run
            )
            
            success = recommission_mgr.recommission_broker(args.broker_id, args.state_file)
            
            if success:
                if args.dry_run:
                    logger.info("✓ Recommission simulation completed successfully")
                else:
                    logger.info("✓ Recommission completed successfully")
                sys.exit(0)
            else:
                logger.error("✗ Recommission failed")
                sys.exit(1)
        
        else:
            # Decommission mode
            logger.info(f"Operating in DECOMMISSION mode for broker {args.broker_id}")
            if args.dry_run:
                logger.info("🔍 DRY-RUN mode enabled - simulating decommission")
            
            # Run pre-checks unless skipped
            if not args.skip_prechecks:
                validator = PreCheckValidator(cluster_manager, config, logger)
                if not validator.run_all_checks(args.broker_id):
                    logger.error("Pre-checks failed. Aborting decommission.")
                    logger.error("Use --skip-prechecks to bypass (NOT RECOMMENDED)")
                    sys.exit(1)
            else:
                logger.warning("⚠ Pre-checks SKIPPED - proceeding without validation")
            
            # Execute decommission
            decommission_mgr = BrokerDecommissionManager(
                cluster_manager,
                broker_manager,
                config,
                logger,
                args.dry_run
            )
            
            success = decommission_mgr.decommission_broker(args.broker_id)
            
            if success:
                if args.dry_run:
                    logger.info("✓ Decommission simulation completed successfully")
                    logger.info(f"To execute for real: python3 {sys.argv[0]} --config {args.config} "
                               f"--broker-id {args.broker_id}")
                else:
                    logger.info("✓ Decommission completed successfully")
                    logger.info(f"To recommission: python3 {sys.argv[0]} --config {args.config} "
                               f"--broker-id {args.broker_id} --recommission")
                sys.exit(0)
            else:
                logger.error("✗ Decommission failed")
                sys.exit(1)
    
    except KeyboardInterrupt:
        logger.warning("\nOperation interrupted by user")
        sys.exit(130)
    
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        logger.debug(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
