#!/bin/bash

# Start Kafka and ZooKeeper for fs2-kafka testing
# 
# Usage: 
#   ./start-kafka.sh [single|cluster] [kafka-version] [--debug]
#
# Examples:
#   ./start-kafka.sh                    # Single broker, minimal output
#   ./start-kafka.sh cluster            # 3-broker cluster, minimal output  
#   ./start-kafka.sh single --debug     # Single broker with detailed logging
#   ./start-kafka.sh cluster --debug    # Cluster with detailed logging

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Parse arguments
MODE="single"
KAFKA_VERSION="1.0.0"
DEBUG=false

# Parse arguments properly
for arg in "$@"; do
    case $arg in
        --debug)
            DEBUG=true
            ;;
        single|cluster)
            MODE=$arg
            ;;
        [0-9]*)
            KAFKA_VERSION=$arg
            ;;
    esac
done

# Load common utilities
source "$SCRIPT_DIR/common-utils.sh"

NETWORK_NAME="fs2-kafka-network"

# Docker platform for Apple Silicon compatibility
PLATFORM="--platform linux/amd64"

# Image configurations
ZOOKEEPER_IMAGE="zookeeper:3.8.4"
KAFKA_IMAGE="wurstmeister/kafka:${KAFKA_VERSION}"

# Ports
ZK_PORT=2181
KAFKA_PORT_1=9092
KAFKA_PORT_2=9192
KAFKA_PORT_3=9292

# Static IP addresses
ZOOKEEPER_IP="172.30.0.10"
BROKER1_IP="172.30.0.11"
BROKER2_IP="172.30.0.12"
BROKER3_IP="172.30.0.13"


cleanup() {
    log "Cleaning up existing containers..."
    docker kill zookeeper broker1 broker2 broker3 2>/dev/null || true
    docker rm zookeeper broker1 broker2 broker3 2>/dev/null || true
    docker network rm $NETWORK_NAME 2>/dev/null || true
}

create_network() {
    log "Creating Docker network: $NETWORK_NAME"
    docker network create --subnet 172.30.0.0/16 $NETWORK_NAME >/dev/null 2>&1 || true
}

start_zookeeper() {
    log "Starting ZooKeeper on IP $ZOOKEEPER_IP..."
    docker run -d $PLATFORM \
        --name zookeeper \
        --network $NETWORK_NAME \
        --ip $ZOOKEEPER_IP \
        -p $ZK_PORT:2181 \
        $ZOOKEEPER_IMAGE >/dev/null

    # Wait for ZooKeeper to be ready
    log "Waiting for ZooKeeper to be ready..."
    for i in $(seq 1 30); do
        if docker exec zookeeper zkServer.sh status 2>/dev/null | grep -q "Mode: standalone"; then
            log "ZooKeeper is ready"
            return 0
        fi
        sleep 2
    done
    info "ERROR: ZooKeeper failed to start"
    return 1
}

start_kafka_broker() {
    local broker_id=$1
    local port=$2
    local broker_name="broker${broker_id}"
    
    # Get broker IP
    local broker_ip
    case $broker_id in
        1) broker_ip=$BROKER1_IP ;;
        2) broker_ip=$BROKER2_IP ;;
        3) broker_ip=$BROKER3_IP ;;
        *) info "ERROR: Invalid broker_id $broker_id"; return 1 ;;
    esac
    
    log "Starting Kafka broker ${broker_id} on IP ${broker_ip}:${port}..."
    
    docker run -d $PLATFORM \
        --name $broker_name \
        --network $NETWORK_NAME \
        --ip $broker_ip \
        -p $port:$port \
        -e KAFKA_BROKER_ID=$broker_id \
        -e KAFKA_ZOOKEEPER_CONNECT=$ZOOKEEPER_IP:2181 \
        -e KAFKA_ADVERTISED_HOST_NAME=$broker_ip \
        -e KAFKA_ADVERTISED_PORT=$port \
        -e KAFKA_PORT=$port \
        -e KAFKA_LOG_RETENTION_HOURS=1 \
        -e KAFKA_LOG_SEGMENT_BYTES=1073741824 \
        -e KAFKA_LOG_RETENTION_BYTES=1073741824 \
        $KAFKA_IMAGE >/dev/null

    # Wait for broker to be ready
    log "Waiting for Kafka broker ${broker_id} to be ready..."
    for i in $(seq 1 30); do
        if docker logs $broker_name 2>&1 | grep -q "started (kafka.server.KafkaServer)"; then
            log "Kafka broker ${broker_id} is ready"
            return 0
        fi
        sleep 2
    done
    info "ERROR: Kafka broker ${broker_id} failed to start"
    return 1
}

start_single_broker() {
    log "Starting single broker Kafka setup..."
    start_kafka_broker 1 $KAFKA_PORT_1
}

start_cluster() {
    log "Starting Kafka cluster (3 brokers)..."
    
    start_kafka_broker 1 $KAFKA_PORT_1
    if [ "$DEBUG" = true ]; then
        log "Waiting for broker 1 to stabilize..."
        sleep 5
    fi
    
    start_kafka_broker 2 $KAFKA_PORT_2
    if [ "$DEBUG" = true ]; then
        log "Waiting for broker 2 to stabilize..."
        sleep 5
    fi
    
    start_kafka_broker 3 $KAFKA_PORT_3
    
    # Brief stabilization wait
    sleep 3
}


create_test_topic() {
    local topic_name="${1:-test-topic-A}"
    local partitions="${2:-1}"
    local replication="${3:-1}"
    
    log "Creating test topic: $topic_name"
    docker exec broker1 kafka-topics.sh \
        --create \
        --topic $topic_name \
        --partitions $partitions \
        --replication-factor $replication \
        --zookeeper zookeeper:2181 || true
}

main() {
    script_start "START-KAFKA"
    log "Starting Kafka setup (mode: $MODE, version: $KAFKA_VERSION)"
    
    cleanup
    create_network
    start_zookeeper
    
    case $MODE in
        "single")
            start_single_broker
            ;;
        "cluster")
            start_cluster
            ;;
        *)
            info "ERROR: Unknown mode '$MODE'. Use 'single' or 'cluster'"
            exit 1
            ;;
    esac
    
    info "Kafka setup complete!"
    if [ "$DEBUG" = true ]; then
        info "ZooKeeper: $ZOOKEEPER_IP:$ZK_PORT (also accessible via localhost:$ZK_PORT)"
        info "Kafka Broker 1: $BROKER1_IP:$KAFKA_PORT_1 (also accessible via localhost:$KAFKA_PORT_1)"
        if [ "$MODE" = "cluster" ]; then
            info "Kafka Broker 2: $BROKER2_IP:$KAFKA_PORT_2 (also accessible via localhost:$KAFKA_PORT_2)"
            info "Kafka Broker 3: $BROKER3_IP:$KAFKA_PORT_3 (also accessible via localhost:$KAFKA_PORT_3)"
        fi
        echo ""
        docker ps --filter "network=$NETWORK_NAME"
    else
        info "ZooKeeper: $ZOOKEEPER_IP:$ZK_PORT"
        if [ "$MODE" = "cluster" ]; then
            info "Brokers: $BROKER1_IP:$KAFKA_PORT_1, $BROKER2_IP:$KAFKA_PORT_2, $BROKER3_IP:$KAFKA_PORT_3"
        else
            info "Broker: $BROKER1_IP:$KAFKA_PORT_1"
        fi
    fi
    script_end "START-KAFKA"
}

# Run main function
main "$@"