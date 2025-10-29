# Kafka Leader Demotion Configuration (Simplified)
# ==================================================
# Now with auto-detection! Only bootstrap_servers is required.

# REQUIRED: Kafka bootstrap servers
# ----------------------------------
bootstrap_servers: "stg-hdpashique101:6667"

# AUTO-DETECTED FIELDS (optional overrides)
# ------------------------------------------
# The following will be auto-detected if not specified:

# 1. Kafka bin path
#    Auto-detected from: /etc/kafka/*/0/kafka-env.sh or find command
#    Override if needed:
# kafka_bin_path: "/usr/odp/3.2.2.0-1/kafka/bin"

# 2. Zookeeper server  
#    Auto-detected from: /etc/kafka/conf/server.properties
#    Override if needed:
# zookeeper_server: "stg-hdpashique103:2181"

# OPTIONAL: OpenTSDB for CPU Monitoring
# --------------------------------------
# If not provided, script will only check disk usage (via kafka-log-dirs.sh)
# If provided, script will also check CPU usage before selecting new leaders
# opentsdb_url: "http://opentsdb-read-no-dp-limit.nixy.stg-drove.phonepe.nb6/api/query"

# Resource Thresholds
# -------------------
cpu_threshold: 80       # Max CPU % for candidate brokers (requires OpenTSDB)
disk_threshold: 85      # Max disk % for candidate brokers (always checked)

# Kafka Configuration
# -------------------
min_isr_required: 2     # Minimum in-sync replicas required

# Timeouts (seconds)
# ------------------
reassignment_timeout: 300
verification_interval: 10

# State Management
# ----------------
state_directory: "/data/kafka_demotion_state"
log_directory: "./logs"

# =============================================================================
# DISK MONITORING - NOW AUTOMATIC!
# =============================================================================
# The script now uses `kafka-log-dirs.sh` to get actual Kafka disk usage.
# No need to specify disk_mount_path anymore!
#
# The script will:
# 1. Query kafka-log-dirs.sh for all brokers
# 2. Calculate total partition sizes per broker
# 3. Warn if any broker is > disk_threshold% full
# 4. Consider disk usage when selecting new leaders
#
# NOTE: Disk usage is always checked (doesn't require OpenTSDB)
# =============================================================================

# =============================================================================
# AUTO-DETECTION DETAILS
# =============================================================================
# 
# Kafka Bin Path Detection (Priority Order):
# 1. Parse /etc/kafka/*/0/kafka-env.sh for CLASSPATH
# 2. Check standard paths: /usr/odp/current/kafka-broker/bin
# 3. Fall back to: find /usr -name kafka-topics.sh
#
# Zookeeper Server Detection:
# 1. Parse /etc/kafka/conf/server.properties
# 2. Look for: zookeeper.connect=host1:2181,host2:2181
# 3. Extract first server from the list
#
# If auto-detection fails, specify values manually above.
# =============================================================================

# =============================================================================
# MINIMAL CONFIGURATION EXAMPLE
# =============================================================================
# For most deployments, you only need:
#
# bootstrap_servers: "your-kafka-host:6667"
#
# That's it! Everything else is auto-detected or uses sensible defaults.
# =============================================================================
