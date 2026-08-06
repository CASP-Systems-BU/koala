#Note: This script is designed to run kafka in KRaft mode.
#Note: You need any default Java version installed on your system
#!/usr/bin/env bash
set -euxo pipefail

# Define Kafka version and download URL
KAFKA_VERSION="3.9.0/kafka_2.13-3.9.0.tgz"
KAFKA_PKG="kafka_2.13-3.9.0.tgz"
KAFKA_DIR="kafka_2.13-3.9.0"
BROKER_IPS=$(echo "$*" | tr -d ' ')
KAFKA_DOWNLOAD_URL="https://archive.apache.org/dist/kafka/$KAFKA_VERSION"
CONFIG="config/kraft/reconfig-server.properties"

# Download & extract Kafka if needed
if [ ! -d "$KAFKA_DIR" ]; then
  if [ ! -f "$KAFKA_PKG" ]; then
    echo "Downloading Kafka..."
    wget $KAFKA_DOWNLOAD_URL
  fi
  echo "Extracting Kafka..."
    tar -xzf $KAFKA_PKG
fi
cd "$KAFKA_DIR"

IFS=',' read -ra IPS <<< "$BROKER_IPS"

VOTERS=""
NODE_ID=1

# Get the primary interface
PRIMARY_IFACE=$(ip route get 1.1.1.1 | awk '{print $5; exit}')

# Check if an interface was found
if [ -z "$PRIMARY_IFACE" ]; then
    echo "ERROR: Could not automatically determine the primary network interface via default route."
    echo "Please check network configuration or set advertised.listeners manually in $CONFIG"
  exit 1
fi

echo "Identified primary interface: $PRIMARY_IFACE"
# Get the private address of that primary interface
CURRENT_IP=$(hostname -I | tr ' ' '\n' | grep -E '^10\.|^172\.|^192\.168\.' | head -n 1)

# Check if private address was found
if [ -z "$CURRENT_IP" ]; then
    echo "ERROR: Could not automatically determine the external IP address."
    echo "Please set advertised.listeners manually in $CONFIG"
     # Fallback to localhost
    CURRENT_IP="localhost"
fi

# Extract the IPs and construct the voters string
for ip in "${IPS[@]}"; do
  ip=$(echo "$ip" | xargs) 
  if [ -z "$VOTERS" ]; then
    VOTERS="${NODE_ID}@${ip}:9093"
  else
    VOTERS="${VOTERS},${NODE_ID}@${ip}:9093"
  fi

  # If current node IP matches, set node.id
  if [[ "$ip" == "${CURRENT_IP}" ]]; then
    sed -i "s|^node.id=.*|node.id=${NODE_ID}|" "$CONFIG"
    echo "Assigned node.id=${NODE_ID} for current node ($CURRENT_IP)"
  fi

  ((NODE_ID++))
done

# Update controller.quorum.voters
if grep -q '^controller.quorum.voters=' "$CONFIG"; then
    sed -i "s|^controller.quorum.voters=.*|controller.quorum.voters=${VOTERS}|" "$CONFIG"
else
    echo "controller.quorum.voters=${VOTERS}" >> "$CONFIG"
fi

echo "Updated controller.quorum.voters=${VOTERS} in $CONFIG"

PARTITIONS=${#IPS[@]}
# Update partition count
sed -i "s|^num.partitions=.*|num.partitions=${PARTITIONS}|" "$CONFIG"

# Find your data dir from the config (defaults to /tmp/kraft-combined-logs)
LOG_DIR=$(grep '^log.dirs=' "$CONFIG" | cut -d'=' -f2)
mkdir -p "$LOG_DIR"

# Format storage if first run
if [ ! -f "$LOG_DIR/meta.properties" ]; then
  echo ">>> Formatting storage (no meta.properties found)"
  # Use a fixed CLUSTER_ID for all nodes in the cluster
  CLUSTER_ID="hi5lupemSWui6TOy7mQlTA"
  bin/kafka-storage.sh format -t "$CLUSTER_ID" -c "$CONFIG"
  echo -e "auto.create.topics.enable=true\nmessage.timestamp.type=LogAppendTime" >> "$CONFIG"

fi

echo "Setting advertised.listeners to IP: $CURRENT_IP"

# set 'listeners='
sed -i "s|^listeners=.*|listeners=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093|" "$CONFIG"

# set 'advertised.listeners='
sed -i "s|^advertised.listeners=.*|advertised.listeners=PLAINTEXT://${CURRENT_IP}:9092,CONTROLLER://${CURRENT_IP}:9093|" "$CONFIG"

echo ">>> Current listener configuration in $CONFIG:"
grep -E "^listeners=|^advertised.listeners=" "$CONFIG"

echo ">>> Starting Kafka server in KRaft mode"
bin/kafka-server-start.sh "$CONFIG"