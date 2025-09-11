# Kafka Test Scripts

This directory contains scripts to manage Kafka and ZooKeeper containers for fs2-kafka testing.

## Prerequisites

- Docker installed and running
- `lsof` command available (for port checking)
- `nc` (netcat) command available (for connectivity testing)

## Scripts

### Core Scripts

#### `start-kafka.sh`

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

#### `stop-kafka.sh`

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

#### `test-kafka.sh`

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

### Development Helper Scripts

#### `kafka-dev.sh` - All-in-One Development Helper

The most convenient script for daily development work. Provides a unified interface for all Kafka operations.

**Usage:**
```bash
./kafka-dev.sh [command] [options]
```

**Common Commands:**
```bash
# Quick start/stop
./kafka-dev.sh start                    # Start single broker
./kafka-dev.sh start cluster            # Start 3-broker cluster  
./kafka-dev.sh stop                     # Stop everything
./kafka-dev.sh restart                  # Restart with same config

# Status and monitoring
./kafka-dev.sh status                   # Detailed system status
./kafka-dev.sh test                     # Run connectivity tests
./kafka-dev.sh logs                     # Show all container logs
./kafka-dev.sh logs broker1             # Show specific container logs

# Topic management
./kafka-dev.sh topic create my-topic                # Create topic (1 partition, 1 replica)
./kafka-dev.sh topic create my-topic 3 2           # Create with 3 partitions, 2 replicas  
./kafka-dev.sh topic list                          # List all topics
./kafka-dev.sh topic describe                      # Describe all topics
./kafka-dev.sh topic describe my-topic             # Describe specific topic
./kafka-dev.sh topic delete my-topic               # Delete topic

# Cleanup
./kafka-dev.sh clean                    # Full cleanup (includes Docker volumes)
```

#### `kafka-status.sh` - Detailed Status Checker

Shows comprehensive status of all Kafka components.

**Usage:**
```bash
./kafka-status.sh [--verbose]
```

**What it shows:**
- Container status (running/stopped)
- Network connectivity
- Port availability  
- ZooKeeper health
- Kafka broker responsiveness
- Topic listing
- Verbose mode: detailed Docker information

**Examples:**
```bash
./kafka-status.sh                      # Basic status check
./kafka-status.sh --verbose            # Detailed status with Docker info
```

#### `kafka-quick-test.sh` - Fast Functionality Test

Quickly verifies Kafka is working by testing basic produce/consume operations.

**Usage:**
```bash
./kafka-quick-test.sh [broker] [--cleanup]
```

**What it tests:**
- Creates a temporary test topic
- Produces a test message
- Consumes and verifies the message
- Checks topic listing
- Optionally tests cluster replication (if cluster mode detected)

**Examples:**
```bash
./kafka-quick-test.sh                          # Test with default broker (localhost:9092)
./kafka-quick-test.sh --cleanup                # Test and cleanup test topic afterward
./kafka-quick-test.sh localhost:9192          # Test specific broker
```

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

## Development Workflow Examples

### Using the Development Helper (Recommended)

**Quick single broker development:**
```bash
./kafka-dev.sh start                    # Start Kafka
./kafka-quick-test.sh                  # Verify it's working
./kafka-dev.sh topic create my-test     # Create your test topic
# Run your fs2-kafka tests here
./kafka-dev.sh logs broker1             # Check logs if needed
./kafka-dev.sh stop                     # Clean shutdown
```

**Cluster development:**
```bash
./kafka-dev.sh start cluster            # Start 3-broker cluster
./kafka-dev.sh status                   # Check cluster health
./kafka-dev.sh topic create replicated-topic 1 3  # Create replicated topic
# Run your cluster tests here
./kafka-dev.sh clean                    # Full cleanup
```

**Daily development cycle:**
```bash
./kafka-dev.sh start                    # Morning: start Kafka
# ... develop and test throughout the day ...
./kafka-dev.sh restart                  # Clean slate if needed
# ... more development ...
./kafka-dev.sh stop                     # Evening: clean shutdown
```

### Using Individual Scripts

**Traditional approach (more control):**
```bash
./stop-kafka.sh                        # Clean slate
./start-kafka.sh single 1.0.0          # Start specific version
./test-kafka.sh                        # Verify connectivity
# Run your tests
./kafka-status.sh --verbose            # Check detailed status
./stop-kafka.sh                        # Cleanup
```