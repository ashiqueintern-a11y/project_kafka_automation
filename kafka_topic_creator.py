#!/usr/bin/env python3
"""
Production-Grade Kafka Topic Creator - Compact Version
======================================================
Streamlined script for creating Kafka topics with comprehensive pre-checks.
Compatible with Kafka 2.8.2

Author: DevOps Team
Version: 2.0.0
"""

import sys
import os
import re
import logging
import time
import subprocess
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import yaml


class KafkaTopicCreator:
    """Kafka topic creator with pre-checks and validation."""
    
    VALID_NAME_PATTERN = re.compile(r'^[a-zA-Z0-9._-]+$')
    MAX_NAME_LENGTH = 249
    
    def __init__(self, config_file: str, log_level: str = "INFO"):
        self.config_file = config_file
        self.config = None
        self.logger = self._setup_logging(log_level)
        self.kafka_bin_path = self._find_kafka_bin()
        self.kafka_topics_cmd = os.path.join(self.kafka_bin_path, 'kafka-topics.sh')
        self.kafka_log_dirs_cmd = os.path.join(self.kafka_bin_path, 'kafka-log-dirs.sh')
        self.bootstrap_servers = None
        
    def _setup_logging(self, log_level: str) -> logging.Logger:
        """Configure logging with file and console handlers."""
        logger = logging.getLogger("KafkaTopicCreator")
        logger.setLevel(getattr(logging, log_level.upper()))
        logger.handlers.clear()
        
        # File handler
        log_file = f"kafka_topic_creator_{time.strftime('%Y%m%d_%H%M%S')}.log"
        fh = logging.FileHandler(log_file)
        fh.setFormatter(logging.Formatter(
            '%(asctime)s | %(levelname)s | %(funcName)s | %(message)s'
        ))
        
        # Console handler
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(logging.Formatter(
            '%(asctime)s | %(levelname)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        ))
        
        logger.addHandler(fh)
        logger.addHandler(ch)
        
        # Now we can log (logger is created)
        logger.info(f"Logging to: {log_file}")
        return logger
    
    def _find_kafka_bin(self) -> str:
        """Find Kafka bin directory by reading kafka-env.sh or searching filesystem."""
        self.logger.debug("Searching for Kafka bin directory...")
        
        # Strategy 1: Read from kafka-env.sh (most reliable for production systems)
        kafka_env_paths = [
            '/etc/kafka/*/0/kafka-env.sh',
            '/etc/kafka/conf/kafka-env.sh',
            '/usr/odp/*/kafka/config/kafka-env.sh',
            '/usr/hdp/*/kafka/config/kafka-env.sh',
        ]
        
        import glob
        for pattern in kafka_env_paths:
            for kafka_env in glob.glob(pattern):
                try:
                    self.logger.debug(f"Checking kafka-env.sh at: {kafka_env}")
                    with open(kafka_env, 'r') as f:
                        for line in f:
                            # Look for CLASSPATH export that contains kafka-broker path
                            if 'CLASSPATH' in line and 'kafka-broker' in line:
                                # Extract path like: /usr/odp/current/kafka-broker/config
                                import re
                                match = re.search(r'(/[^\s:]+/kafka-broker)', line)
                                if match:
                                    kafka_base = match.group(1)
                                    bin_path = os.path.join(kafka_base, 'bin')
                                    if os.path.exists(bin_path):
                                        self.logger.info(f"Found Kafka bin from kafka-env.sh: {bin_path}")
                                        return bin_path
                except Exception as e:
                    self.logger.debug(f"Failed to read {kafka_env}: {e}")
        
        # Strategy 2: Check symbolic link /usr/odp/current/kafka-broker (ODP standard)
        odp_current = '/usr/odp/current/kafka-broker/bin'
        if os.path.exists(odp_current):
            self.logger.info(f"Found Kafka bin at: {odp_current}")
            return odp_current
        
        # Strategy 3: Check symbolic link /usr/hdp/current/kafka-broker (HDP standard)
        hdp_current = '/usr/hdp/current/kafka-broker/bin'
        if os.path.exists(hdp_current):
            self.logger.info(f"Found Kafka bin at: {hdp_current}")
            return hdp_current
        
        # Strategy 4: Use 'which' to find kafka-topics.sh
        try:
            result = subprocess.run(['which', 'kafka-topics.sh'], 
                                  capture_output=True, text=True, timeout=5)
            if result.returncode == 0 and result.stdout.strip():
                tool_path = result.stdout.strip()
                bin_path = os.path.dirname(tool_path)
                self.logger.info(f"Found Kafka bin via 'which': {bin_path}")
                return bin_path
        except Exception as e:
            self.logger.debug(f"'which' command failed: {e}")
        
        # Strategy 5: Search filesystem for kafka-topics.sh
        self.logger.info("Searching filesystem for kafka-topics.sh (this may take a moment)...")
        try:
            result = subprocess.run(
                ['find', '/usr/', '/opt/', '-name', 'kafka-topics.sh', '-type', 'f'],
                capture_output=True, text=True, timeout=30
            )
            if result.returncode == 0 and result.stdout.strip():
                paths = result.stdout.strip().split('\n')
                for path in paths:
                    if os.access(path, os.X_OK):
                        bin_path = os.path.dirname(path)
                        self.logger.info(f"Found Kafka bin via filesystem search: {bin_path}")
                        return bin_path
        except Exception as e:
            self.logger.debug(f"Filesystem search failed: {e}")
        
        # If nothing found, show error
        self.logger.error("="*80)
        self.logger.error("KAFKA INSTALLATION NOT FOUND")
        self.logger.error("="*80)
        self.logger.error("Could not locate Kafka installation.")
        self.logger.error("")
        self.logger.error("Searched locations:")
        self.logger.error("  - /etc/kafka/*/0/kafka-env.sh")
        self.logger.error("  - /usr/odp/current/kafka-broker/bin")
        self.logger.error("  - /usr/hdp/current/kafka-broker/bin")
        self.logger.error("  - System PATH")
        self.logger.error("  - Filesystem search in /usr/ and /opt/")
        self.logger.error("")
        self.logger.error("Please ensure Kafka is installed on this system.")
        self.logger.error("="*80)
        sys.exit(1)
    
    def _run_cmd(self, cmd: List[str]) -> Tuple[bool, str, str]:
        """Execute command and return (success, stdout, stderr)."""
        try:
            self.logger.debug(f"Running: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            return (result.returncode == 0, result.stdout.strip(), result.stderr.strip())
        except subprocess.TimeoutExpired:
            self.logger.error("Command timed out after 30 seconds")
            return (False, "", "Command timeout")
        except FileNotFoundError as e:
            self.logger.error(f"Command not found: {cmd[0]}")
            self.logger.error("The kafka-topics.sh tool is not available at the detected path")
            self.logger.error(f"Detected path: {self.kafka_topics_cmd}")
            self.logger.error("")
            self.logger.error("Solutions:")
            self.logger.error("  1. Verify Kafka installation location")
            self.logger.error("  2. Re-run with correct KAFKA_HOME:")
            self.logger.error(f"     python3 {sys.argv[0]} {self.config_file} /correct/kafka/path")
            return (False, "", str(e))
        except Exception as e:
            self.logger.error(f"Command execution error: {e}")
            return (False, "", str(e))
    
    def load_config(self) -> bool:
        """Load and validate YAML configuration."""
        try:
            self.logger.info(f"Loading config: {self.config_file}")
            
            if not Path(self.config_file).exists():
                self.logger.error(f"Config file not found: {self.config_file}")
                return False
            
            with open(self.config_file, 'r') as f:
                self.config = yaml.safe_load(f)
            
            # Validate required fields
            required = {
                'topic': ['name', 'partitions', 'replication_factor'],
                'cluster': ['bootstrap_servers']
            }
            
            for section, fields in required.items():
                if section not in self.config:
                    self.logger.error(f"Missing section: {section}")
                    return False
                for field in fields:
                    if field not in self.config[section]:
                        self.logger.error(f"Missing {section}.{field}")
                        return False
            
            self.bootstrap_servers = ','.join(self.config['cluster']['bootstrap_servers'])
            self.logger.info("Config loaded successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Config load error: {e}")
            return False
    
    def validate_topic_name(self, name: str) -> Tuple[bool, str]:
        """Validate topic name."""
        if not name or len(name) == 0:
            return False, "Topic name cannot be empty"
        
        if len(name) > self.MAX_NAME_LENGTH:
            return False, f"Name exceeds {self.MAX_NAME_LENGTH} characters"
        
        if not self.VALID_NAME_PATTERN.match(name):
            return False, "Name must contain only ASCII alphanumerics, '.', '_', '-'"
        
        if name in ['.', '..']:
            return False, "Name cannot be '.' or '..'"
        
        return True, None
    
    def check_connectivity(self) -> bool:
        """Test connection to Kafka brokers."""
        self.logger.info(f"Testing connectivity: {self.bootstrap_servers}")
        cmd = [self.kafka_topics_cmd, '--bootstrap-server', self.bootstrap_servers, '--list']
        success, _, stderr = self._run_cmd(cmd)
        
        if not success:
            self.logger.error(f"Connection failed: {stderr}")
            return False
        
        self.logger.info("Connected successfully")
        return True
    
    def check_topic_exists(self, topic: str) -> bool:
        """Check if topic already exists."""
        self.logger.info(f"Checking if topic '{topic}' exists")
        cmd = [self.kafka_topics_cmd, '--bootstrap-server', self.bootstrap_servers, '--list']
        success, stdout, _ = self._run_cmd(cmd)
        
        if not success:
            return True  # Fail-safe
        
        exists = topic in stdout.split('\n')
        if exists:
            self.logger.error(f"Topic '{topic}' already exists")
        else:
            self.logger.info("Topic does not exist (OK)")
        
        return exists
    
    def get_broker_count(self) -> int:
        """Get number of available brokers."""
        self.logger.info("Checking broker count")
        cmd = [self.kafka_topics_cmd, '--bootstrap-server', self.bootstrap_servers, 
               '--describe', '--exclude-internal']
        success, stdout, _ = self._run_cmd(cmd)
        
        if not success:
            # Fall back to bootstrap servers count
            count = len(self.config['cluster']['bootstrap_servers'])
            self.logger.warning(f"Using bootstrap servers count: {count}")
            return count
        
        # Parse broker IDs from output
        broker_ids = set()
        for line in stdout.split('\n'):
            if 'Replicas:' in line:
                replicas = line.split('Replicas:')[1].split()[0]
                for bid in replicas.split(','):
                    try:
                        broker_ids.add(int(bid))
                    except ValueError:
                        pass
        
        count = len(broker_ids) if broker_ids else len(self.config['cluster']['bootstrap_servers'])
        self.logger.info(f"Available brokers: {count}")
        return count
    
    def validate_replication_factor(self, rf: int, broker_count: int) -> bool:
        """Validate replication factor against broker count."""
        self.logger.info(f"Validating RF={rf} against {broker_count} brokers")
        
        if rf < 1:
            self.logger.error("Replication factor must be at least 1")
            return False
        
        if rf > broker_count:
            self.logger.error(f"RF ({rf}) exceeds available brokers ({broker_count})")
            return False
        
        if rf < 3:
            self.logger.warning(f"RF={rf} is below production recommendation of 3")
        
        self.logger.info(f"RF validation passed")
        return True
    
    def check_under_replicated_partitions(self) -> int:
        """Check for under-replicated partitions."""
        self.logger.info("Checking under-replicated partitions")
        cmd = [self.kafka_topics_cmd, '--bootstrap-server', self.bootstrap_servers,
               '--describe', '--under-replicated-partitions']
        success, stdout, _ = self._run_cmd(cmd)
        
        if not success:
            self.logger.warning("Could not check under-replicated partitions")
            return -1
        
        urp_count = len([l for l in stdout.split('\n') if l.strip() and 'Topic:' in l])
        
        if urp_count > 0:
            self.logger.warning(f"Found {urp_count} under-replicated partitions")
        else:
            self.logger.info("No under-replicated partitions (OK)")
        
        return urp_count
    
    def check_disk_space(self) -> bool:
        """Check disk space using kafka-log-dirs.sh."""
        self.logger.info("Checking disk space using kafka-log-dirs.sh")
        
        cmd = [
            self.kafka_log_dirs_cmd,
            '--bootstrap-server', self.bootstrap_servers,
            '--describe'
        ]
        
        success, stdout, stderr = self._run_cmd(cmd)
        
        if not success:
            self.logger.warning(f"Could not check disk space: {stderr}")
            return True  # Don't fail the workflow, just warn
        
        try:
            # Parse JSON output from kafka-log-dirs.sh
            import json
            data = json.loads(stdout)
            
            total_issues = 0
            
            # Iterate through brokers
            for broker_info in data.get('brokers', []):
                broker_id = broker_info.get('broker', 'unknown')
                log_dirs = broker_info.get('logDirs', [])
                
                for log_dir in log_dirs:
                    path = log_dir.get('logDir', 'unknown')
                    error = log_dir.get('error', None)
                    
                    if error:
                        self.logger.error(f"Broker {broker_id} - Error in {path}: {error}")
                        total_issues += 1
                        continue
                    
                    # Get size information (in bytes)
                    total_bytes = log_dir.get('totalBytes', 0)
                    usable_bytes = log_dir.get('usableBytes', 0)
                    
                    if total_bytes > 0:
                        total_gb = total_bytes / (1024**3)
                        usable_gb = usable_bytes / (1024**3)
                        used_gb = total_gb - usable_gb
                        used_pct = (used_gb / total_gb * 100) if total_gb > 0 else 0
                        
                        self.logger.info(
                            f"Broker {broker_id} - {path}: "
                            f"{usable_gb:.1f}GB free / {total_gb:.1f}GB total ({used_pct:.1f}% used)"
                        )
                        
                        # Warnings
                        if usable_gb < 10:
                            self.logger.warning(
                                f"Broker {broker_id} - LOW DISK SPACE: Only {usable_gb:.1f}GB free in {path}"
                            )
                            total_issues += 1
                        elif used_pct > 85:
                            self.logger.warning(
                                f"Broker {broker_id} - HIGH DISK USAGE: {used_pct:.1f}% used in {path}"
                            )
                            total_issues += 1
            
            if total_issues > 0:
                self.logger.warning(f"Found {total_issues} disk space issue(s)")
            else:
                self.logger.info("Disk space check passed")
            
            return True
            
        except json.JSONDecodeError as e:
            self.logger.warning(f"Could not parse kafka-log-dirs output: {e}")
            self.logger.debug(f"Output was: {stdout}")
            return True  # Don't fail workflow
        except Exception as e:
            self.logger.warning(f"Disk space check error: {e}")
            return True  # Don't fail workflow
    
    def create_topic(self) -> bool:
        """Create the Kafka topic."""
        topic = self.config['topic']['name']
        partitions = self.config['topic']['partitions']
        rf = self.config['topic']['replication_factor']
        
        self.logger.info(f"Creating topic '{topic}' (partitions={partitions}, RF={rf})")
        
        cmd = [
            self.kafka_topics_cmd,
            '--bootstrap-server', self.bootstrap_servers,
            '--create',
            '--topic', topic,
            '--partitions', str(partitions),
            '--replication-factor', str(rf)
        ]
        
        # Add topic configs
        if 'config' in self.config and self.config['config']:
            for key, value in self.config['config'].items():
                cmd.extend(['--config', f"{key}={value}"])
                self.logger.debug(f"Config: {key}={value}")
        
        success, stdout, stderr = self._run_cmd(cmd)
        
        if not success:
            if 'already exists' in stderr.lower():
                self.logger.error("Topic already exists")
            else:
                self.logger.error(f"Creation failed: {stderr}")
            return False
        
        self.logger.info(f"Topic '{topic}' created successfully")
        return True
    
    def verify_topic(self, topic: str) -> bool:
        """Verify topic was created correctly."""
        self.logger.info(f"Verifying topic '{topic}'")
        time.sleep(2)  # Wait for metadata propagation
        
        cmd = [self.kafka_topics_cmd, '--bootstrap-server', self.bootstrap_servers,
               '--describe', '--topic', topic]
        success, stdout, stderr = self._run_cmd(cmd)
        
        if not success:
            self.logger.error(f"Verification failed: {stderr}")
            return False
        
        # Verify partition count
        expected_partitions = self.config['topic']['partitions']
        actual_partitions = len([l for l in stdout.split('\n') if 'Partition:' in l])
        
        if actual_partitions != expected_partitions:
            self.logger.error(f"Partition mismatch: expected {expected_partitions}, got {actual_partitions}")
            return False
        
        # Verify replication factor
        expected_rf = self.config['topic']['replication_factor']
        for line in stdout.split('\n'):
            if 'Replicas:' in line:
                replicas = line.split('Replicas:')[1].split()[0]
                actual_rf = len(replicas.split(','))
                if actual_rf != expected_rf:
                    self.logger.error(f"RF mismatch: expected {expected_rf}, got {actual_rf}")
                    return False
                break
        
        self.logger.info(f"Verification passed: {actual_partitions} partitions, RF={expected_rf}")
        return True
    
    def run(self) -> bool:
        """Execute the complete workflow."""
        try:
            self.logger.info("="*80)
            self.logger.info("Kafka Topic Creation Workflow")
            self.logger.info("="*80)
            
            # Step 1: Load config
            if not self.load_config():
                self.logger.error("FAILED: Config load")
                return False
            
            topic_name = self.config['topic']['name']
            rf = self.config['topic']['replication_factor']
            
            # Step 2: Validate topic name
            valid, error = self.validate_topic_name(topic_name)
            if not valid:
                self.logger.error(f"FAILED: Topic name - {error}")
                return False
            self.logger.info(f"Topic name '{topic_name}' is valid")
            
            # Step 3: Check connectivity
            if not self.check_connectivity():
                self.logger.error("FAILED: Connectivity")
                return False
            
            # Step 4: Check if exists
            if self.check_topic_exists(topic_name):
                self.logger.error("FAILED: Topic exists")
                return False
            
            # Step 5: Check broker count
            broker_count = self.get_broker_count()
            if broker_count < rf:
                self.logger.error(f"FAILED: Need {rf} brokers for RF={rf}, found {broker_count}")
                return False
            self.logger.info(f"Broker check passed: {broker_count} >= {rf}")
            
            # Step 6: Validate RF
            if not self.validate_replication_factor(rf, broker_count):
                self.logger.error("FAILED: RF validation")
                return False
            
            # Step 7: Check cluster health
            urp_count = self.check_under_replicated_partitions()
            if urp_count > 0:
                self.logger.warning("Cluster has under-replicated partitions")
            
            # Step 8: Check disk space
            self.check_disk_space()
            
            # Step 9: Create topic
            if not self.create_topic():
                self.logger.error("FAILED: Topic creation")
                return False
            
            # Step 10: Verify
            if not self.verify_topic(topic_name):
                self.logger.error("FAILED: Verification")
                return False
            
            self.logger.info("="*80)
            self.logger.info(f"SUCCESS: Topic '{topic_name}' created")
            self.logger.info("="*80)
            return True
            
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}", exc_info=True)
            return False


def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        print("Usage: python kafka_topic_creator.py <config.yaml> [log_level]")
        print("\nExamples:")
        print("  python kafka_topic_creator.py topic.yaml")
        print("  python kafka_topic_creator.py topic.yaml DEBUG")
        print("\nNote: Kafka bin directory is auto-detected from:")
        print("  1. /etc/kafka/*/0/kafka-env.sh")
        print("  2. /usr/odp/current/kafka-broker/bin")
        print("  3. /usr/hdp/current/kafka-broker/bin")
        print("  4. System PATH")
        print("  5. Filesystem search")
        sys.exit(1)
    
    config_file = sys.argv[1]
    log_level = sys.argv[2] if len(sys.argv) > 2 else "INFO"
    
    creator = KafkaTopicCreator(config_file, log_level)
    success = creator.run()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
