================================================================================
2025-10-30 02:13:13,348 - INFO - PHASE 2: OPENTSDB METRICS COLLECTION & ENHANCED ANALYSIS
2025-10-30 02:13:13,349 - INFO - ================================================================================
2025-10-30 02:13:13,349 - INFO -
[ANALYSIS] Retrieving Cluster Metadata...
2025-10-30 02:13:16,343 - INFO - Found 4 broker(s): [1001, 1002, 1003, 1004]
2025-10-30 02:13:16,343 - INFO - Found 17 topic(s)
2025-10-30 02:13:16,343 - INFO -
Partition Distribution:
2025-10-30 02:13:16,343 - INFO -   Broker 1001: 27 leaders, 77 replicas
2025-10-30 02:13:16,343 - INFO -   Broker 1002: 27 leaders, 76 replicas
2025-10-30 02:13:16,344 - INFO -   Broker 1003: 27 leaders, 81 replicas
2025-10-30 02:13:16,344 - INFO -   Broker 1004: 26 leaders, 87 replicas
2025-10-30 02:13:16,344 - INFO -
[ANALYSIS] Analyzing Partition Skewness with OpenTSDB Metrics...
2025-10-30 02:13:16,344 - INFO -
[METRICS] Collecting Broker Metrics using kafka-log-dirs.sh...
2025-10-30 02:13:19,267 - INFO -
📊 Disk Usage Summary (from kafka-log-dirs.sh):
2025-10-30 02:13:19,267 - INFO - ============================================================
2025-10-30 02:13:19,267 - INFO - Broker 1001: 0.00 GB used
2025-10-30 02:13:19,267 - INFO -   Hostname: stg-hdpashique103.phonepe.nb6
2025-10-30 02:13:19,267 - INFO -   Estimated Disk Usage: 30.0%
2025-10-30 02:13:19,268 - INFO -   CPU: 30.00% (default)
2025-10-30 02:13:19,268 - INFO -   Health Score: 30.00
2025-10-30 02:13:19,268 - INFO - Broker 1004: 0.00 GB used
2025-10-30 02:13:19,268 - INFO -   Hostname: stg-hdpashique104.phonepe.nb6
2025-10-30 02:13:19,268 - INFO -   Estimated Disk Usage: 30.0%
2025-10-30 02:13:19,268 - INFO -   CPU: 30.00% (default)
2025-10-30 02:13:19,268 - INFO -   Health Score: 30.00
2025-10-30 02:13:19,268 - INFO - Broker 1003: 0.00 GB used
2025-10-30 02:13:19,269 - INFO -   Hostname: stg-hdpashique101.phonepe.nb6
2025-10-30 02:13:19,269 - INFO -   Estimated Disk Usage: 30.0%
2025-10-30 02:13:19,269 - INFO -   CPU: 30.00% (default)
2025-10-30 02:13:19,269 - INFO -   Health Score: 30.00
2025-10-30 02:13:19,270 - INFO - Broker 1002: 0.00 GB used
2025-10-30 02:13:19,270 - INFO -   Hostname: stg-hdpashique102.phonepe.nb6
2025-10-30 02:13:19,270 - INFO -   Estimated Disk Usage: 30.0%
2025-10-30 02:13:19,270 - INFO -   CPU: 30.00% (default)
2025-10-30 02:13:19,270 - INFO -   Health Score: 30.00
2025-10-30 02:13:19,270 - INFO - ============================================================
2025-10-30 02:13:19,270 - INFO -
[METRICS] Collecting Partition Metrics...
2025-10-30 02:13:19,271 - INFO -
--- Leader Distribution Analysis ---
2025-10-30 02:13:19,271 - INFO - Broker 1001:
2025-10-30 02:13:19,271 - INFO -   Leaders: 27 (expected: 26.75, deviation: 0.93%)
2025-10-30 02:13:19,271 - INFO -   Health Score: 30.00
2025-10-30 02:13:19,271 - INFO - Broker 1002:
2025-10-30 02:13:19,271 - INFO -   Leaders: 27 (expected: 26.75, deviation: 0.93%)
2025-10-30 02:13:19,272 - INFO -   Health Score: 30.00
2025-10-30 02:13:19,272 - INFO - Broker 1003:
2025-10-30 02:13:19,272 - INFO -   Leaders: 27 (expected: 26.75, deviation: 0.93%)
2025-10-30 02:13:19,272 - INFO -   Health Score: 30.00
2025-10-30 02:13:19,272 - INFO - Broker 1004:
2025-10-30 02:13:19,272 - INFO -   Leaders: 26 (expected: 26.75, deviation: 2.80%)
2025-10-30 02:13:19,272 - INFO -   Health Score: 30.00
2025-10-30 02:13:19,272 - INFO - ✅ Leader distribution is balanced
2025-10-30 02:13:19,272 - INFO -
--- Replica Distribution Analysis (Disk + Count Aware) ---
2025-10-30 02:13:19,273 - INFO - Broker 1001:
2025-10-30 02:13:19,273 - INFO -   Disk Usage: 30.00%
2025-10-30 02:13:19,273 - INFO -   CPU Usage: 30.00%
2025-10-30 02:13:19,273 - INFO -   Replicas: 77
2025-10-30 02:13:19,273 - INFO -   ✅ ELIGIBLE for partition reassignment
2025-10-30 02:13:19,273 - INFO - Broker 1002:
2025-10-30 02:13:19,273 - INFO -   Disk Usage: 30.00%
2025-10-30 02:13:19,273 - INFO -   CPU Usage: 30.00%
2025-10-30 02:13:19,273 - INFO -   Replicas: 76
2025-10-30 02:13:19,273 - INFO -   ✅ ELIGIBLE for partition reassignment
2025-10-30 02:13:19,274 - INFO - Broker 1003:
2025-10-30 02:13:19,274 - INFO -   Disk Usage: 30.00%
2025-10-30 02:13:19,274 - INFO -   CPU Usage: 30.00%
2025-10-30 02:13:19,274 - INFO -   Replicas: 81
2025-10-30 02:13:19,274 - INFO -   ✅ ELIGIBLE for partition reassignment
2025-10-30 02:13:19,274 - INFO - Broker 1004:
2025-10-30 02:13:19,274 - INFO -   Disk Usage: 30.00%
2025-10-30 02:13:19,274 - INFO -   CPU Usage: 30.00%
2025-10-30 02:13:19,275 - INFO -   Replicas: 87
2025-10-30 02:13:19,275 - INFO -   ✅ ELIGIBLE for partition reassignment
2025-10-30 02:13:19,275 - INFO -
📊 Disk Range: 30.00% - 30.00% (diff: 0.00%)
2025-10-30 02:13:19,275 - INFO -
📊 Replica Count Analysis:
2025-10-30 02:13:19,275 - INFO - Total Replicas: 321
2025-10-30 02:13:19,275 - INFO - Expected per Broker: 80.25
2025-10-30 02:13:19,275 - INFO - Range: 76 - 87 (diff: 11)
2025-10-30 02:13:19,275 - INFO - ✅ Replica distribution is balanced (both disk and count)
2025-10-30 02:13:19,275 - INFO -
================================================================================
2025-10-30 02:13:19,276 - INFO - PHASE 3: INTELLIGENT REBALANCING (TWO-PHASE APPROACH)
2025-10-30 02:13:19,276 - INFO - ================================================================================
2025-10-30 02:13:19,276 - INFO -
✅ CLUSTER STATUS: HEALTHY & BALANCED
root@stg-hdpashique101:/home/sre/project_kafka_automation[nb6][stg]#
