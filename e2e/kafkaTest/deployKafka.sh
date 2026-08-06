Note: This script is designed to run kafka in KRaft mode. It has scripts for both Linux and macOS systems.
You can select the appropriate commands based on your operating system.

Note: You need any default Java version installed on your system
#!/usr/bin/env bash
set -euxo pipefail

# Define Kafka version and download URL
KAFKA_VERSION="3.9.0/kafka_2.13-3.9.0.tgz"
KAFKA_PKG="kafka_2.13-3.9.0.tgz"
KAFKA_DIR="kafka_2.13-3.9.0"
KAFKA_DOWNLOAD_URL="https://dlcdn.apache.org/kafka/$KAFKA_VERSION"
CONFIG="config/kraft/reconfig-server.properties"

# Download & extract Kafka if needed
if [ ! -d "$KAFKA_DIR" ]; then
  if [ ! -f "$KAFKA_PKG" ]; then
    echo "Downloading Kafka..."
    # for linux systems, use wget
    # wget $KAFKA_DOWNLOAD_URL

    # for macOS systems, use curl
    curl -sSL -o "$KAFKA_PKG" "$KAFKA_DOWNLOAD_URL"
  fi
  echo "Extracting Kafka..."
    tar -xzf $KAFKA_PKG
fi
cd "$KAFKA_DIR"

# find your data dir from the config (defaults to /tmp/kraft-combined-logs)
LOG_DIR=$(grep '^log.dirs=' "$CONFIG" | cut -d'=' -f2)
mkdir -p "$LOG_DIR"

# Format storage if first run
if [ ! -f "$LOG_DIR/meta.properties" ]; then
  echo ">>> Formatting storage (no meta.properties found)"
  CLUSTER_ID=$(bin/kafka-storage.sh random-uuid)
  bin/kafka-storage.sh format --standalone -t "$CLUSTER_ID" -c "$CONFIG"
  echo -e "auto.create.topics.enable=true\nmessage.timestamp.type=LogAppendTime" >> "$CONFIG"

fi

# for linux systems, use the following command to get the primary interface
#PRIMARY_IFACE=$(ip route get 1.1.1.1 | awk '{print $5; exit}')

# for macOS systems, use the following command to get the primary interface
PRIMARY_IFACE=$(route -n get default 2>/dev/null | awk '/interface: / {print $2; exit}')


# Check if an interface was found
if [ -z "$PRIMARY_IFACE" ]; then
    echo "ERROR: Could not automatically determine the primary network interface via default route."
    echo "Please check network configuration or set advertised.listeners manually in $CONFIG"
  exit 1
fi

echo "Identified primary interface: $PRIMARY_IFACE"

# Get the IPv4 address of that primary interface
# for Linux systems
#EXTERNAL_IP=$(ip -4 addr show "$PRIMARY_IFACE" | grep -oP '(?<=inet\s)\d+(\.\d+){3}')

# for macOS systems
EXTERNAL_IP=$(ipconfig getifaddr "$PRIMARY_IFACE")

if [ -z "$EXTERNAL_IP" ]; then
    echo "ERROR: Could not automatically determine the external IP address."
    echo "Please set advertised.listeners manually in $CONFIG"
    EXTERNAL_IP="localhost" # Fallback to localhost
fi

echo "Setting advertised.listeners to IP: $EXTERNAL_IP"

# set 'listeners='
# for Linux systems
# sed -i "s|^listeners=.*|listeners=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093|" "$CONFIG"

# for macOS systems
sed -i '' -e \
  's|^listeners=.*|listeners=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093|' "$CONFIG"

# set 'advertised.listeners='
# for Linux systems
#sed -i "s|^advertised.listeners=.*|advertised.listeners=PLAINTEXT://${EXTERNAL_IP}:9092,CONTROLLER://${EXTERNAL_IP}:9093|" "$CONFIG"

# for macOS systems
sed -i '' -e \
  "s|^advertised.listeners=.*|advertised.listeners=PLAINTEXT://${EXTERNAL_IP}:9092,CONTROLLER://${EXTERNAL_IP}:9093|" "$CONFIG"

echo ">>> Current listener configuration in $CONFIG:"
grep -E "^listeners=|^advertised.listeners=" "$CONFIG"

echo ">>> Starting Kafka server in KRaft mode"
bin/kafka-server-start.sh "$CONFIG"
