root@stg-hdpashique101:/home/sre/project_kafka_automation[nb6][stg]# python3 kafka_demoter.py --config leader-demoter-input.yaml --broker-id 1001 --dry-run

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

2025-10-30 13:06:48,662 - INFO - Logging initialized. Log file: ./logs/kafka_decommission_20251030_130648.log
2025-10-30 13:06:48,662 - INFO - Loading configuration from leader-demoter-input.yaml
2025-10-30 13:06:48,664 - INFO - Configuration loaded from leader-demoter-input.yaml
2025-10-30 13:06:48,664 - INFO - kafka_bin_path not configured, attempting auto-detection...
2025-10-30 13:06:48,665 - INFO - ✓ Auto-detected kafka_bin_path: /usr/odp/current/kafka-broker/bin
2025-10-30 13:06:48,665 - INFO - zookeeper_server not configured, attempting auto-detection...
2025-10-30 13:06:48,666 - INFO - ✓ Auto-detected zookeeper_server: stg-hdpashique101.phonepe.nb6:2181
2025-10-30 13:06:48,666 - INFO - kafka_server_config not configured, attempting auto-detection...
2025-10-30 13:06:48,666 - INFO - ✓ Auto-detected kafka_server_config: /etc/kafka/conf/server.properties
2025-10-30 13:06:48,666 - INFO - ✓ Configuration validation passed
2025-10-30 13:06:48,666 - INFO -   Bootstrap servers: stg-hdpashique103:6667
2025-10-30 13:06:48,666 - INFO -   Kafka bin path: /usr/odp/current/kafka-broker/bin
2025-10-30 13:06:48,666 - INFO -   Zookeeper server: stg-hdpashique101.phonepe.nb6:2181
2025-10-30 13:06:48,666 - INFO -   Server config: /etc/kafka/conf/server.properties
2025-10-30 13:06:48,666 - INFO -   State directory: /data/kafka_demotion_state
2025-10-30 13:06:48,666 - INFO -   Log directory: ./logs
2025-10-30 13:06:48,666 - INFO - Initializing Kafka cluster manager
2025-10-30 13:06:48,667 - INFO - Initializing broker manager
2025-10-30 13:06:48,667 - INFO -
2025-10-30 13:06:48,667 - INFO - ======================================================================
2025-10-30 13:06:48,667 - INFO - OPERATION: DECOMMISSION BROKER 1001
2025-10-30 13:06:48,667 - INFO - MODE: DRY-RUN (simulation only)
2025-10-30 13:06:48,667 - INFO - ======================================================================
2025-10-30 13:06:48,667 - INFO -
2025-10-30 13:06:48,667 - INFO - ======================================================================
2025-10-30 13:06:48,667 - INFO - STARTING PRE-CHECK VALIDATION
2025-10-30 13:06:48,667 - INFO - ======================================================================
2025-10-30 13:06:48,667 - INFO -
──────────────────────────────────────────────────────────────────────
2025-10-30 13:06:48,668 - INFO - CHECK: Controller Health
2025-10-30 13:06:48,668 - INFO - ──────────────────────────────────────────────────────────────────────
2025-10-30 13:06:48,668 - INFO - Checking controller health
2025-10-30 13:06:50,608 - INFO - Controller is broker 1001
2025-10-30 13:06:50,609 - INFO - ✓ Controller Health: PASSED
2025-10-30 13:06:50,609 - INFO -
──────────────────────────────────────────────────────────────────────
2025-10-30 13:06:50,609 - INFO - CHECK: Broker Existence
2025-10-30 13:06:50,609 - INFO - ──────────────────────────────────────────────────────────────────────
2025-10-30 13:06:50,609 - INFO - Fetching all broker IDs
2025-10-30 13:06:54,680 - INFO - Broker 1001 hostname: stg-hdpashique103.phonepe.nb6
2025-10-30 13:06:56,566 - INFO - Broker 1002 hostname: stg-hdpashique102.phonepe.nb6
2025-10-30 13:06:58,670 - INFO - Broker 1003 hostname: stg-hdpashique101.phonepe.nb6
2025-10-30 13:07:00,787 - INFO - Broker 1004 hostname: stg-hdpashique104.phonepe.nb6
2025-10-30 13:07:00,787 - INFO - Found 4 brokers in cluster
2025-10-30 13:07:00,787 - INFO - Broker 1001 found: stg-hdpashique103.phonepe.nb6
2025-10-30 13:07:00,788 - INFO - ✓ Broker Existence: PASSED
2025-10-30 13:07:00,788 - INFO -
──────────────────────────────────────────────────────────────────────
2025-10-30 13:07:00,788 - INFO - CHECK: Under-Replicated Partitions
2025-10-30 13:07:00,788 - INFO - ──────────────────────────────────────────────────────────────────────
2025-10-30 13:07:00,788 - INFO - Fetching partition metadata
2025-10-30 13:07:03,872 - INFO - Retrieved metadata for 17 topics
2025-10-30 13:07:03,872 - INFO - Total partitions: 107
2025-10-30 13:07:03,872 - INFO - Total under-replicated partitions: 0
2025-10-30 13:07:03,872 - INFO - No under-replicated partitions found
2025-10-30 13:07:03,872 - INFO - ✓ Under-Replicated Partitions: PASSED
2025-10-30 13:07:03,872 - INFO -
──────────────────────────────────────────────────────────────────────
2025-10-30 13:07:03,872 - INFO - CHECK: Topic Min ISR Configuration
2025-10-30 13:07:03,873 - INFO - ──────────────────────────────────────────────────────────────────────
2025-10-30 13:07:03,873 - INFO - Fetching partition metadata
2025-10-30 13:07:06,752 - INFO - Retrieved metadata for 17 topics
2025-10-30 13:07:06,752 - INFO - Total partitions: 107
2025-10-30 13:07:55,473 - INFO - ✓ Topic Min ISR Configuration: PASSED
2025-10-30 13:07:55,473 - INFO -
──────────────────────────────────────────────────────────────────────
2025-10-30 13:07:55,473 - INFO - CHECK: Follower ISR Status
2025-10-30 13:07:55,473 - INFO - ──────────────────────────────────────────────────────────────────────
2025-10-30 13:07:55,474 - INFO - Fetching partition metadata
2025-10-30 13:07:58,434 - INFO - Retrieved metadata for 17 topics
2025-10-30 13:07:58,434 - INFO - Total partitions: 107
2025-10-30 13:07:58,436 - INFO - ✓ Follower ISR Status: PASSED
