now i got error like 
2025-10-30 01:38:54,520 - INFO - Bootstrap Server: stg-hdpashique101:6667
2025-10-30 01:38:54,521 - INFO - Zookeeper: stg-hdpashique101.phonepe.nb6:2181,stg-hdpashique102.phonepe.nb6:2181,stg-hdpashique103.phonepe.nb6:2181
2025-10-30 01:38:54,521 - INFO - Kafka Bin: /usr/odp/current/kafka-broker/bin
2025-10-30 01:38:54,521 - INFO - Skew Threshold: 15.0%
2025-10-30 01:38:54,521 - INFO - Replica Skew Threshold: 20.0%
2025-10-30 01:38:54,521 - INFO - Disk Threshold: 85.0%
2025-10-30 01:38:54,521 - INFO - Batch Size: 50 partitions
2025-10-30 01:38:54,521 - INFO - Log File: /var/log/kafka-tools/partition_balancer_20251030_013854.log
2025-10-30 01:38:54,521 - INFO - ================================================================================
2025-10-30 01:38:54,521 - INFO -
================================================================================
2025-10-30 01:38:54,522 - INFO - KAFKA PARTITION BALANCER v4.2.1 - ENHANCED
2025-10-30 01:38:54,522 - INFO - ================================================================================
2025-10-30 01:38:54,522 - INFO -
================================================================================
2025-10-30 01:38:54,522 - INFO - PHASE 1: BROKER DISCOVERY & PRE-FLIGHT CHECKS
2025-10-30 01:38:54,522 - INFO - ================================================================================
2025-10-30 01:38:54,522 - INFO -
[DISCOVERY] Discovering Brokers from Zookeeper...
2025-10-30 01:38:56,565 - INFO - Raw broker IDs: [1001, 1002, 1003, 1004]
2025-10-30 01:38:56,565 - INFO - Found 4 broker(s): [1001, 1002, 1003, 1004]
2025-10-30 01:38:56,565 - INFO - Fetching details for broker 1001...
2025-10-30 01:38:58,609 - INFO -   Broker 1001: stg-hdpashique103.phonepe.nb6:6667
2025-10-30 01:38:58,609 - INFO - Fetching details for broker 1002...
2025-10-30 01:39:00,738 - INFO -   Broker 1002: stg-hdpashique102.phonepe.nb6:6667
2025-10-30 01:39:00,739 - INFO - Fetching details for broker 1003...
2025-10-30 01:39:02,653 - INFO -   Broker 1003: stg-hdpashique101.phonepe.nb6:6667
2025-10-30 01:39:02,653 - INFO - Fetching details for broker 1004...
2025-10-30 01:39:04,647 - INFO -   Broker 1004: stg-hdpashique104.phonepe.nb6:6667
2025-10-30 01:39:04,648 - INFO -
✅ Successfully discovered 4 broker(s)
2025-10-30 01:39:04,648 - INFO -
[PRECHECK 1/4] Checking Cluster Health...
2025-10-30 01:39:07,348 - INFO - ✅ Cluster is reachable. Found 4 broker(s)
2025-10-30 01:39:07,348 - INFO -    - Broker 1004:stg-hdpashique104.phonepe.nb6
2025-10-30 01:39:07,349 - INFO -    - Broker 1001:stg-hdpashique103.phonepe.nb6
2025-10-30 01:39:07,349 - INFO -    - Broker 1003:stg-hdpashique101.phonepe.nb6
2025-10-30 01:39:07,349 - INFO -    - Broker 1002:stg-hdpashique102.phonepe.nb6
2025-10-30 01:39:07,349 - INFO -
[PRECHECK 2/4] Checking for Under-Replicated Partitions...
2025-10-30 01:39:10,355 - INFO - ✅ No under-replicated partitions found
2025-10-30 01:39:10,355 - INFO -
[PRECHECK 3/4] Checking Kafka Controller Status...
2025-10-30 01:39:13,145 - INFO - ✅ Kafka controller is active
2025-10-30 01:39:13,146 - INFO -
[PRECHECK 4/4] Checking for Ongoing Reassignments...
2025-10-30 01:39:16,008 - INFO - ✅ No ongoing reassignments
2025-10-30 01:39:16,008 - INFO -
✅ All pre-checks passed!
2025-10-30 01:39:16,008 - INFO -
================================================================================
2025-10-30 01:39:16,009 - INFO - PHASE 2: OPENTSDB METRICS COLLECTION & ENHANCED ANALYSIS
2025-10-30 01:39:16,009 - INFO - ================================================================================
2025-10-30 01:39:16,009 - INFO -
[ANALYSIS] Retrieving Cluster Metadata...
2025-10-30 01:39:18,914 - INFO - Found 4 broker(s): [1001, 1002, 1003, 1004]
2025-10-30 01:39:18,914 - INFO - Found 17 topic(s)
2025-10-30 01:39:18,914 - INFO -
Partition Distribution:
2025-10-30 01:39:18,914 - INFO -   Broker 1001: 27 leaders, 77 replicas
2025-10-30 01:39:18,914 - INFO -   Broker 1002: 27 leaders, 76 replicas
2025-10-30 01:39:18,914 - INFO -   Broker 1003: 27 leaders, 81 replicas
2025-10-30 01:39:18,915 - INFO -   Broker 1004: 26 leaders, 87 replicas
2025-10-30 01:39:18,915 - INFO -
[ANALYSIS] Analyzing Partition Skewness with OpenTSDB Metrics...
2025-10-30 01:39:18,915 - INFO -
[METRICS] Collecting Broker Metrics using kafka-log-dirs.sh...
2025-10-30 01:39:21,924 - INFO -
📊 Disk Usage Summary (from kafka-log-dirs.sh):
2025-10-30 01:39:21,924 - INFO - ============================================================
2025-10-30 01:39:21,924 - INFO - Broker 1001: 0.00 GB used
2025-10-30 01:39:21,925 - INFO -   Hostname: stg-hdpashique103.phonepe.nb6
2025-10-30 01:39:21,925 - INFO -   Estimated Disk Usage: 30.0%
2025-10-30 01:39:21,925 - INFO -   CPU: 30.00% (default)
2025-10-30 01:39:21,925 - INFO -   Health Score: 30.00
2025-10-30 01:39:21,925 - INFO - Broker 1004: 0.00 GB used
2025-10-30 01:39:21,925 - INFO -   Hostname: stg-hdpashique104.phonepe.nb6
2025-10-30 01:39:21,925 - INFO -   Estimated Disk Usage: 30.0%
2025-10-30 01:39:21,925 - INFO -   CPU: 30.00% (default)
2025-10-30 01:39:21,925 - INFO -   Health Score: 30.00
2025-10-30 01:39:21,925 - INFO - Broker 1003: 0.00 GB used
2025-10-30 01:39:21,926 - INFO -   Hostname: stg-hdpashique101.phonepe.nb6
2025-10-30 01:39:21,926 - INFO -   Estimated Disk Usage: 30.0%
2025-10-30 01:39:21,926 - INFO -   CPU: 30.00% (default)
2025-10-30 01:39:21,926 - INFO -   Health Score: 30.00
2025-10-30 01:39:21,926 - INFO - Broker 1002: 0.00 GB used
2025-10-30 01:39:21,926 - INFO -   Hostname: stg-hdpashique102.phonepe.nb6
2025-10-30 01:39:21,926 - INFO -   Estimated Disk Usage: 30.0%
2025-10-30 01:39:21,926 - INFO -   CPU: 30.00% (default)
2025-10-30 01:39:21,926 - INFO -   Health Score: 30.00
2025-10-30 01:39:21,926 - INFO - ============================================================
2025-10-30 01:39:21,927 - INFO -
[METRICS] Collecting Partition Metrics...
2025-10-30 01:39:21,927 - INFO -
--- Leader Distribution Analysis ---
2025-10-30 01:39:21,927 - INFO - Broker 1001:
2025-10-30 01:39:21,927 - INFO -   Leaders: 27 (expected: 26.75, deviation: 0.93%)
2025-10-30 01:39:21,928 - INFO -   Health Score: 30.00
2025-10-30 01:39:21,928 - INFO - Broker 1002:
2025-10-30 01:39:21,928 - INFO -   Leaders: 27 (expected: 26.75, deviation: 0.93%)
2025-10-30 01:39:21,928 - INFO -   Health Score: 30.00
2025-10-30 01:39:21,928 - INFO - Broker 1003:
2025-10-30 01:39:21,928 - INFO -   Leaders: 27 (expected: 26.75, deviation: 0.93%)
2025-10-30 01:39:21,928 - INFO -   Health Score: 30.00
2025-10-30 01:39:21,928 - INFO - Broker 1004:
2025-10-30 01:39:21,928 - INFO -   Leaders: 26 (expected: 26.75, deviation: 2.80%)
2025-10-30 01:39:21,928 - INFO -   Health Score: 30.00
2025-10-30 01:39:21,929 - INFO - ✅ Leader distribution is balanced
2025-10-30 01:39:21,929 - INFO -
--- Replica Distribution Analysis (Disk + Count Aware) ---
2025-10-30 01:39:21,929 - INFO - Broker 1001:
2025-10-30 01:39:21,929 - INFO -   Disk Usage: 30.00%
2025-10-30 01:39:21,929 - INFO -   CPU Usage: 30.00%
2025-10-30 01:39:21,929 - INFO -   Replicas: 77
2025-10-30 01:39:21,929 - INFO -   ✅ ELIGIBLE for partition reassignment
2025-10-30 01:39:21,929 - INFO - Broker 1002:
2025-10-30 01:39:21,929 - INFO -   Disk Usage: 30.00%
2025-10-30 01:39:21,929 - INFO -   CPU Usage: 30.00%
2025-10-30 01:39:21,929 - INFO -   Replicas: 76
2025-10-30 01:39:21,930 - INFO -   ✅ ELIGIBLE for partition reassignment
2025-10-30 01:39:21,930 - INFO - Broker 1003:
2025-10-30 01:39:21,930 - INFO -   Disk Usage: 30.00%
2025-10-30 01:39:21,930 - INFO -   CPU Usage: 30.00%
2025-10-30 01:39:21,930 - INFO -   Replicas: 81
2025-10-30 01:39:21,930 - INFO -   ✅ ELIGIBLE for partition reassignment
2025-10-30 01:39:21,930 - INFO - Broker 1004:
2025-10-30 01:39:21,930 - INFO -   Disk Usage: 30.00%
2025-10-30 01:39:21,930 - INFO -   CPU Usage: 30.00%
2025-10-30 01:39:21,930 - INFO -   Replicas: 87
2025-10-30 01:39:21,930 - INFO -   ✅ ELIGIBLE for partition reassignment
2025-10-30 01:39:21,930 - INFO -
📊 Disk Range: 30.00% - 30.00% (diff: 0.00%)
2025-10-30 01:39:21,931 - INFO -
📊 Replica Count Analysis:
2025-10-30 01:39:21,931 - INFO - Total Replicas: 321
2025-10-30 01:39:21,931 - INFO - Expected per Broker: 80.25
2025-10-30 01:39:21,931 - INFO - Range: 76 - 87 (diff: 11)
2025-10-30 01:39:21,931 - INFO - ✅ Replica distribution is balanced (both disk and count)
2025-10-30 01:39:21,931 - ERROR -

Unexpected error: cannot unpack non-iterable NoneType object
Traceback (most recent call last):
  File "kafka_skewnes.py", line 1966, in main
    success = balancer.run_comprehensive_check(
  File "kafka_skewnes.py", line 1844, in run_comprehensive_check
    analysis = self.analyze_skewness_with_metrics(metadata)
  File "kafka_skewnes.py", line 934, in analyze_skewness_with_metrics
    replica_skewness, eligible_brokers = self._analyze_replica_skewness_enhanced(
TypeError: cannot unpack non-iterable NoneType object
root@stg-hdpashique101:/home/sre/project_kafka_automation[nb6][stg]#



