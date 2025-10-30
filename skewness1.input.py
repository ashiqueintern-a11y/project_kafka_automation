# Kafka Partition Balancer Configuration File
# Version: 4.2.1

# Kafka Cluster Configuration
kafka:
  # Bootstrap server (required)
  bootstrap_servers: "stg-hdpashique101:6667"

# OpenTSDB Configuration (for CPU metrics only)
opentsdb:
  # OpenTSDB server URL
  url: "http://opentsdb.phonepe.com:4242"
  
  # Metric names in OpenTSDB
  metrics:
    # CPU idle metric (will be converted to usage: 100 - idle)
    cpu: "system.cpu.idle"

# Skewness Detection Thresholds
thresholds:
  # Leader distribution skew threshold (%)
  # If any broker deviates more than this percentage from expected, it's skewed
  skew_percent: 15.0
  
  # Replica count skew threshold (%)
  # If any broker has replica count deviation beyond this, it's skewed
  replica_skew_percent: 20.0
  
  # Maximum disk usage percentage allowed (%)
  # Brokers exceeding this are marked as critical
  disk_max_percent: 85.0
  
  # Maximum disk usage difference between brokers (%)
  # If difference exceeds this, cluster has disk skewness
  disk_max_diff_percent: 20.0

# Partition Reassignment Configuration
reassignment:
  # Replication throttle in bytes per second
  # Limits data transfer rate during rebalancing
  # 100 MB/s = 104857600 bytes/s
  throttle_bytes_per_sec: 104857600
  
  # Batch size: number of partitions to reassign in one batch
  # Smaller batches = safer, slower; Larger batches = faster, more risk
  batch_size: 50
  
  # Wait time between batches (seconds)
  # Allows cluster to stabilize between batches
  batch_wait_seconds: 60

# Advanced Configuration
advanced:
  # Maximum retries for leader election
  leader_election_max_retries: 2
  
  # Wait time after leader election before verification (seconds)
  leader_election_wait_seconds: 30
  
  # Tolerance percentage for replica distribution (%)
  # Allows 15% deviation from perfect balance before triggering rebalancing
  # This prevents over-rebalancing and broker draining
  tolerance_percent: 15.0# Kafka Partition Balancer Configuration File
# Version: 4.2.1

# Kafka Cluster Configuration
kafka:
  # Bootstrap server (required)
  bootstrap_servers: "stg-hdpashique101:6667"
  
  # Zookeeper connection (optional - will auto-detect from server.properties if not provided)
  zookeeper_connect: "stg-hdpashique101.phonepe.nb6:2181,stg-hdpashique102.phonepe.nb6:2181,stg-hdpashique103.phonepe.nb6:2181"
  
  # Kafka bin directory (optional - will auto-detect if not provided)
  bin_dir: "/usr/odp/current/kafka-broker/bin"

# OpenTSDB Configuration (for CPU and Disk metrics)
opentsdb:
  # OpenTSDB server URL
  url: "http://opentsdb.phonepe.com:4242"
  
  # Disk mount path to monitor
  disk_mount_path: "/data"
  
  # Metric names in OpenTSDB
  metrics:
    # CPU idle metric (will be converted to usage: 100 - idle)
    cpu: "system.cpu.idle"
    
    # Disk usage percentage metric
    disk: "disk.field.used_percent"

# Skewness Detection Thresholds
thresholds:
  # Leader distribution skew threshold (%)
  # If any broker deviates more than this percentage from expected, it's skewed
  skew_percent: 15.0
  
  # Replica count skew threshold (%)
  # If any broker has replica count deviation beyond this, it's skewed
  replica_skew_percent: 20.0
  
  # Maximum disk usage percentage allowed (%)
  # Brokers exceeding this are marked as critical
  disk_max_percent: 85.0
  
  # Maximum disk usage difference between brokers (%)
  # If difference exceeds this, cluster has disk skewness
  disk_max_diff_percent: 20.0

# Partition Reassignment Configuration
reassignment:
  # Replication throttle in bytes per second
  # Limits data transfer rate during rebalancing
  # 100 MB/s = 104857600 bytes/s
  throttle_bytes_per_sec: 104857600
  
  # Batch size: number of partitions to reassign in one batch
  # Smaller batches = safer, slower; Larger batches = faster, more risk
  batch_size: 50
  
  # Wait time between batches (seconds)
  # Allows cluster to stabilize between batches
  batch_wait_seconds: 60

# Advanced Configuration
advanced:
  # Maximum retries for leader election
  leader_election_max_retries: 2
  
  # Wait time after leader election before verification (seconds)
  leader_election_wait_seconds: 30
  
  # Tolerance percentage for replica distribution (%)
  # Allows 15% deviation from perfect balance before triggering rebalancing
  # This prevents over-rebalancing and broker draining
  tolerance_percent: 15.0
