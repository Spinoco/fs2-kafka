# Kafka Test Scripts

This directory contains scripts to manage Kafka and ZooKeeper containers for fs2-kafka testing.

## Prerequisites

- Docker installed and running
- `lsof` command available (for port checking)
- `nc` (netcat) command available (for connectivity testing)

## Scripts

### `start-kafka.sh`

Starts Kafka and ZooKeeper containers for testing.

**Usage:**
```bash
./start-kafka.sh [single|cluster] [kafka-version]
```

**Parameters:**
- `mode` (optional): `single` (default) or `cluster`
- `kafka-version` (optional): Kafka version to use (default: `1.0.0`)

**Examples:**
```bash
# Start single broker with default version (1.0.0)
./start-kafka.sh

# Start single broker explicitly
./start-kafka.sh single

# Start 3-broker cluster
./start-kafka.sh cluster

# Start with specific Kafka version
./start-kafka.sh single 0.10.2.0
./start-kafka.sh cluster 0.11.0.1
```

**What it does:**
- Creates Docker network `fs2-kafka-network`
- Starts ZooKeeper on port 2181
- Starts Kafka broker(s):
  - Single mode: broker1 on port 9092
  - Cluster mode: broker1 (9092), broker2 (9192), broker3 (9292)
- Creates default test topic `test-topic-A`
- Uses Intel platform (`--platform linux/amd64`) for Apple Silicon compatibility

### `stop-kafka.sh`

Stops and cleans up all Kafka and ZooKeeper containers.

**Usage:**
```bash
./stop-kafka.sh
```

**What it does:**
- Stops all Kafka brokers gracefully
- Stops ZooKeeper
- Removes all containers
- Removes Docker network
- Cleans up orphaned containers
- Shows final status

### `test-kafka.sh`

Tests Kafka connectivity and basic operations.

**Usage:**
```bash
./test-kafka.sh
```

**What it tests:**
- ZooKeeper connectivity and status
- Kafka broker accessibility and readiness
- Port connectivity (9092, 9192, 9292)
- Topic creation and listing
- Message production
- Clean up test artifacts

## Docker Configuration

### Network
- **Network Name**: `fs2-kafka-network`
- **Subnet**: `172.30.0.0/16`
- **Driver**: bridge

### Ports
- **ZooKeeper**: 2181
- **Kafka Broker 1**: 9092
- **Kafka Broker 2**: 9192 (cluster mode only)
- **Kafka Broker 3**: 9292 (cluster mode only)

### Images Used
- **ZooKeeper**: `zookeeper:3.8.4`
- **Kafka**: `wurstmeister/kafka:{version}`

### Environment Variables
Key Kafka configuration:
- `KAFKA_BROKER_ID`: Unique broker ID (1, 2, 3)
- `KAFKA_ZOOKEEPER_CONNECT`: zookeeper:2181
- `KAFKA_ADVERTISED_HOST_NAME`: 127.0.0.1
- `KAFKA_ADVERTISED_PORT`: Broker-specific port
- `KAFKA_LOG_RETENTION_HOURS`: 1 (for testing)

## Supported Kafka Versions

The scripts support wurstmeister Kafka images for versions:
- 0.10.0.0
- 0.10.1.0
- 0.10.2.0
- 0.11.0.0
- 0.11.0.1
- 1.0.0 (default)

## Troubleshooting

### Port Conflicts
If you get port binding errors:
```bash
./stop-kafka.sh  # Clean up first
lsof -i :2181    # Check what's using ZooKeeper port
lsof -i :9092    # Check what's using Kafka port
```

### Connection Issues
If brokers can't connect:
1. Ensure Docker is running
2. Check firewall settings
3. Verify no other services are using the ports
4. Run `./test-kafka.sh` for detailed diagnostics

### Container Startup Issues
Check logs:
```bash
docker logs zookeeper
docker logs broker1
docker logs broker2  # if running cluster
docker logs broker3  # if running cluster
```

### Network Issues
Verify network setup:
```bash
docker network ls
docker network inspect fs2-kafka-network
```

## Integration with Tests

These scripts are designed to replace the Docker management code in the Scala tests:

```scala
// Use these scripts for setup/teardown
Process("./scripts/start-kafka.sh single").!!
// ... run tests ...
Process("./scripts/stop-kafka.sh").!!
```

## Example Workflow

```bash
# Clean start
./stop-kafka.sh

# Start single broker
./start-kafka.sh single

# Verify everything works
./test-kafka.sh

# Run your fs2-kafka tests here
# sbt test

# Clean up
./stop-kafka.sh
```

For cluster testing:
```bash
./start-kafka.sh cluster
./test-kafka.sh
# sbt 'testOnly *ClusterSpec'
./stop-kafka.sh
```