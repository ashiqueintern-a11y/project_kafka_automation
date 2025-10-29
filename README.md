root@stg-hdpashique101:/home/sre/project_kafka_automation[nb6][stg]# python3 kafka_skewnes.py --config skewness.input.yaml --dry-run
2025-10-30 01:18:03,135 - INFO - Configuration loaded from: skewness.input.yaml
2025-10-30 01:18:03,135 - INFO - Auto-detecting Zookeeper connection from server.properties...
2025-10-30 01:18:03,135 - INFO - ✅ Found Zookeeper from /etc/kafka/conf/server.properties: stg-hdpashique101.phonepe.nb6:2181,stg-hdpashique102.phonepe.nb6:2181,stg-hdpashique103.phonepe.nb6:2181
2025-10-30 01:18:03,135 - INFO - Auto-detecting Kafka bin directory...
2025-10-30 01:18:03,140 - INFO - Found kafka-env.sh: /etc/kafka/3.2.2.0-1/0/kafka-env.sh
2025-10-30 01:18:03,142 - INFO - ✅ Found via kafka-env.sh: /usr/odp/current/kafka-broker/bin
2025-10-30 01:18:03,142 - INFO - ================================================================================
2025-10-30 01:18:03,142 - INFO - Kafka Partition Balancer v4.3.0 (Auto-Detect + kafka-log-dirs.sh)
2025-10-30 01:18:03,142 - INFO - Bootstrap Server: stg-hdpashique101:6667
2025-10-30 01:18:03,142 - INFO - Zookeeper: stg-hdpashique101.phonepe.nb6:2181,stg-hdpashique102.phonepe.nb6:2181,stg-hdpashique103.phonepe.nb6:2181
2025-10-30 01:18:03,142 - INFO - Kafka Bin: /usr/odp/current/kafka-broker/bin
2025-10-30 01:18:03,142 - INFO - Skew Threshold: 15.0%
2025-10-30 01:18:03,142 - INFO - Replica Skew Threshold: 20.0%
2025-10-30 01:18:03,142 - INFO - Disk Threshold: 85.0%
2025-10-30 01:18:03,143 - INFO - Batch Size: 50 partitions
2025-10-30 01:18:03,143 - INFO - Log File: /var/log/kafka-tools/partition_balancer_20251030_011803.log
2025-10-30 01:18:03,143 - INFO - ================================================================================
2025-10-30 01:18:03,143 - INFO -
================================================================================
2025-10-30 01:18:03,143 - INFO - KAFKA PARTITION BALANCER v4.0 - ENHANCED
2025-10-30 01:18:03,143 - INFO - ================================================================================
2025-10-30 01:18:03,143 - INFO -
================================================================================
2025-10-30 01:18:03,143 - INFO - PHASE 1: BROKER DISCOVERY & PRE-FLIGHT CHECKS
2025-10-30 01:18:03,143 - INFO - ================================================================================
2025-10-30 01:18:03,144 - INFO -
[DISCOVERY] Discovering Brokers from Zookeeper...
2025-10-30 01:18:05,139 - INFO - Raw broker IDs: [1001, 1002, 1003, 1004]
2025-10-30 01:18:05,140 - INFO - Found 4 broker(s): [1001, 1002, 1003, 1004]
2025-10-30 01:18:05,140 - INFO - Fetching details for broker 1001...
2025-10-30 01:18:07,114 - INFO -   Broker 1001: stg-hdpashique103.phonepe.nb6:6667
2025-10-30 01:18:07,114 - INFO - Fetching details for broker 1002...
2025-10-30 01:18:09,057 - INFO -   Broker 1002: stg-hdpashique102.phonepe.nb6:6667
2025-10-30 01:18:09,058 - INFO - Fetching details for broker 1003...
2025-10-30 01:18:11,065 - INFO -   Broker 1003: stg-hdpashique101.phonepe.nb6:6667
2025-10-30 01:18:11,065 - INFO - Fetching details for broker 1004...
2025-10-30 01:18:13,140 - INFO -   Broker 1004: stg-hdpashique104.phonepe.nb6:6667
2025-10-30 01:18:13,141 - INFO -
✅ Successfully discovered 4 broker(s)
2025-10-30 01:18:13,141 - INFO -
[PRECHECK 1/4] Checking Cluster Health...
2025-10-30 01:18:15,871 - INFO - ✅ Cluster is reachable. Found 4 broker(s)
2025-10-30 01:18:15,871 - INFO -    - Broker 1003:stg-hdpashique101.phonepe.nb6
2025-10-30 01:18:15,872 - INFO -    - Broker 1001:stg-hdpashique103.phonepe.nb6
2025-10-30 01:18:15,872 - INFO -    - Broker 1004:stg-hdpashique104.phonepe.nb6
2025-10-30 01:18:15,872 - INFO -    - Broker 1002:stg-hdpashique102.phonepe.nb6
2025-10-30 01:18:15,872 - INFO -
[PRECHECK 2/4] Checking for Under-Replicated Partitions...
2025-10-30 01:18:18,672 - INFO - ✅ No under-replicated partitions found
2025-10-30 01:18:18,672 - INFO -
[PRECHECK 3/4] Checking Kafka Controller Status...
2025-10-30 01:18:21,378 - INFO - ✅ Kafka controller is active
2025-10-30 01:18:21,379 - INFO -
[PRECHECK 4/4] Checking for Ongoing Reassignments...
2025-10-30 01:18:24,195 - INFO - ✅ No ongoing reassignments
2025-10-30 01:18:24,196 - INFO -
✅ All pre-checks passed!
2025-10-30 01:18:24,196 - INFO -
================================================================================
2025-10-30 01:18:24,196 - INFO - PHASE 2: OPENTSDB METRICS COLLECTION & ENHANCED ANALYSIS
2025-10-30 01:18:24,196 - INFO - ================================================================================
2025-10-30 01:18:24,196 - INFO -
[ANALYSIS] Retrieving Cluster Metadata...
2025-10-30 01:18:27,183 - INFO - Found 4 broker(s): [1001, 1002, 1003, 1004]
2025-10-30 01:18:27,183 - INFO - Found 17 topic(s)
2025-10-30 01:18:27,184 - INFO -
Partition Distribution:
2025-10-30 01:18:27,184 - INFO -   Broker 1001: 27 leaders, 77 replicas
2025-10-30 01:18:27,184 - INFO -   Broker 1002: 27 leaders, 76 replicas
2025-10-30 01:18:27,184 - INFO -   Broker 1003: 27 leaders, 81 replicas
2025-10-30 01:18:27,184 - INFO -   Broker 1004: 26 leaders, 87 replicas
2025-10-30 01:18:27,184 - INFO -
[ANALYSIS] Analyzing Partition Skewness with OpenTSDB Metrics...
2025-10-30 01:18:27,184 - INFO -
[METRICS] Collecting Broker Metrics using kafka-log-dirs.sh...
2025-10-30 01:18:27,184 - ERROR -

Unexpected error: run_kafka_command() got an unexpected keyword argument 'timeout'
Traceback (most recent call last):
  File "kafka_skewnes.py", line 1971, in main
    success = balancer.run_comprehensive_check(
  File "kafka_skewnes.py", line 1849, in run_comprehensive_check
    analysis = self.analyze_skewness_with_metrics(metadata)
  File "kafka_skewnes.py", line 909, in analyze_skewness_with_metrics
    broker_metrics = self.collect_broker_metrics(brokers)
  File "kafka_skewnes.py", line 732, in collect_broker_metrics
    success, stdout, stderr = self.run_kafka_command(command, timeout=60)
TypeError: run_kafka_command() got an unexpected keyword argument 'timeout'
root@stg-hdpashique101:/home/sre/project_kafka_automation[nb6][stg]#






