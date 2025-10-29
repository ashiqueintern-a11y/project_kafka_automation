i want to add one more thing to the script like 
now the script is taking Disk: It queries OpenTSDB for the disk.field.used_percent metric for a specific disk_mount_path (defined in your config file) on the broker hostname. It calculates the average disk usage over the last configured period.

i want it to change it to use the  use the kafka-log-dirs.sh + bootstrap-server and describe like the file i given and also remove the disk path input we no longer need it.

this is the format json file pasted (txt)  the kafka provide i given as file now from it we need to get the disk usage of each broker and check if If any broker is >85% full  print warning in log you can get the disk uage per broker like by 🧩 1. What the JSON shows Your output:

{"version":1,"brokers":[
  {"broker":1001, "logDirs":[{"logDir":"/data/kafka-logs","error":null,"partitions":[
       {"partition":"secound-2","size":0, ...},
       {"partition":"example.events.prod-9","size":0, ...},
       ...
  ]}]},
  {"broker":1002, "logDirs":[ ... ]},
  {"broker":1003, "logDirs":[ ... ]},
  {"broker":1004, "logDirs":[ ... ]}
]}
Each broker object includes: * broker: broker ID (from broker.id) * logDirs: list of Kafka log directories (usually /data/kafka-logs) * Inside each logDir, a list of partitions, with a size field in bytes for each partition So to find per-broker disk usage → 👉 you need to sum all size fields per broker. 🧮 2. Quick one-liner with jq If your system has jq (a JSON parser), this is the simplest and most accurate way:

kafka-log-dirs.sh --bootstrap-server stg-hdpashique101:6667 --describe \
  | jq -r '
    .brokers[] | 
    {broker: .broker, total_bytes: ([.logDirs[].partitions[].size] | add)}'
Sample output:

{
  "broker": 1001,
  "total_bytes": 838860800
}
{
  "broker": 1002,
  "total_bytes": 629145600
}
{
  "broker": 1003,
  "total_bytes": 104857600
}
{
  "broker": 1004,
  "total_bytes": 0
}
This gives you the total Kafka data size per broker, aggregated across all topics/partitions. 🧠 3. Convert bytes → human readable (GB/MB) You can extend that jq expression to convert bytes to GB:

kafka-log-dirs.sh --bootstrap-server stg-hdpashique101:6667 --describe \
  | jq -r '
    .brokers[] | 
    .broker as $id | 
    ([.logDirs[].partitions[].size] | add) as $bytes | 
    {broker: $id, size_GB: ($bytes / 1024 / 1024 / 1024)}
  '
Output:

{
  "broker": 1001,
  "size_GB": 12.3456
}
{
  "broker": 1002,
  "size_GB": 8.7654
}
...
🧰 4. Without jq (pure bash / awk fallback) If you don’t have jq, you can still extract approximate totals:

kafka-log-dirs.sh --bootstrap-server stg-hdpashique101:6667 --describe \
  | grep '"broker":\|size' \
  | awk '
    /"broker":/ {b=$2; gsub(/,/, "", b)} 
    /"size":/ {sub(/[^0-9]/, "", $2); size[b]+=$2} 
    END {for (i in size) print "Broker " i ": " size[i]/1024/1024/1024 " GB"}'
2,  changing the kafka home dir finding the path for kafka-topics.sh and others  use the 
file  cat /etc/kafka/3.2.2.0-1/0/kafka-env.sh like 
root@stg-hdpashique101:/home/sre[nb6][stg]# cat /etc/kafka/3.2.2.0-1/0/kafka-env.sh
#!/bin/bash
# Set KAFKA specific environment variables here.
# The java implementation to use.
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=$PATH:$JAVA_HOME/bin
export PID_DIR=/var/run/kafka
export LOG_DIR=/var/log/kafka
export KAFKA_KERBEROS_PARAMS=
# Add kafka sink to classpath and related depenencies
if [ -e "/usr/lib/ambari-metrics-kafka-sink/ambari-metrics-kafka-sink.jar" ]; then
  export CLASSPATH=$CLASSPATH:/usr/lib/ambari-metrics-kafka-sink/ambari-metrics-kafka-sink.jar
  export CLASSPATH=$CLASSPATH:/usr/lib/ambari-metrics-kafka-sink/lib/*
fi
      export CLASSPATH=$CLASSPATH:/usr/odp/current/kafka-broker/config

and from here we can find the path like here is /usr/odp/current/kafka-broker/bin 
if it fail get by doing find /usr/ -name <name of .sh> and  
also remove the input for it for script

3, remove the input for zookeeper server also and get the zookeeper server from the 
root@stg-hdpashique101:/data/kafka_demotion_state[nb6][stg]# cat /etc/kafka/conf/server.properties | grep zookeeper.connect
zookeeper.connect=stg-hdpashique101.phonepe.nb6:2181,stg-hdpashique102.phonepe.nb6:2181,stg-hdpashique103.phonepe.nb6:2181
zookeeper.connection.timeout.ms=25000

here we have the details of zookeeper
