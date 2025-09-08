#!/bin/bash

# Test Kafka connectivity and basic operations
#
# Usage: 
#   ./test-kafka.sh         # Minimal output - just pass/fail
#   ./test-kafka.sh --debug # Detailed logging and diagnostics

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Check for debug flag
DEBUG=false
for arg in "$@"; do
    if [ "$arg" = "--debug" ]; then
        DEBUG=true
        break
    fi
done

# Load common utilities
source "$SCRIPT_DIR/common-utils.sh"

test_zookeeper() {
    log "Testing ZooKeeper connectivity..."
    if docker exec zookeeper zkServer.sh status 2>/dev/null | grep -q "Mode: standalone"; then
        log "ZooKeeper is running and accessible"
        return 0
    else
        log "ERROR: ZooKeeper is not accessible"
        return 1
    fi
}

test_kafka_broker() {
    local broker_name=$1
    local port=$2
    
    log "Testing Kafka broker: $broker_name (port $port)..."
    
    # Check if broker is running
    if ! docker ps --filter "name=$broker_name" --format "{{.Names}}" | grep -q "^$broker_name$"; then
        log "ERROR: Broker $broker_name is not running"
        return 1
    fi
    
    # Check if broker is ready
    if docker logs $broker_name 2>&1 | grep -q "started (kafka.server.KafkaServer)"; then
        log "Broker $broker_name is running and ready"
    else
        log "WARNING: Broker $broker_name is running but may not be ready yet"
    fi
    
    # Test port connectivity
    if nc -z localhost $port 2>/dev/null; then
        log "Port $port is accessible"
    else
        log "ERROR: Port $port is not accessible"
        return 1
    fi
    
    return 0
}

test_topics() {
    log "Testing topic operations..."
    
    # List topics
    log "Listing topics:"
    docker exec broker1 kafka-topics.sh --list --zookeeper zookeeper:2181 2>/dev/null || {
        log "ERROR: Failed to list topics"
        return 1
    }
    
    # Determine replication factor based on number of running brokers
    local running_brokers=0
    for broker in broker1 broker2 broker3; do
        if docker ps --filter "name=$broker" --format "{{.Names}}" | grep -q "^$broker$"; then
            running_brokers=$((running_brokers + 1))
        fi
    done
    
    local replication_factor=1
    if [ $running_brokers -ge 3 ]; then
        replication_factor=3
        log "Detected cluster mode ($running_brokers brokers), using replication factor $replication_factor"
    else
        log "Detected single broker mode ($running_brokers brokers), using replication factor $replication_factor"
    fi
    
    # Create a test topic
    local test_topic="connectivity-test-$(date +%s)"
    log "Creating test topic: $test_topic (replication: $replication_factor)"
    docker exec broker1 kafka-topics.sh \
        --create \
        --topic $test_topic \
        --partitions 1 \
        --replication-factor $replication_factor \
        --zookeeper zookeeper:2181 2>/dev/null || {
        log "ERROR: Failed to create test topic"
        return 1
    }
    
    # Verify topic was created
    if docker exec broker1 kafka-topics.sh --list --zookeeper zookeeper:2181 2>/dev/null | grep -q "$test_topic"; then
        log "Test topic created successfully"
    else
        log "ERROR: Test topic was not created"
        return 1
    fi
    
    # Test message production/consumption
    log "Testing message production..."
    echo "test-message-$(date +%s)" | docker exec -i broker1 kafka-console-producer.sh \
        --broker-list localhost:9092 \
        --topic $test_topic 2>/dev/null || {
        log "ERROR: Failed to produce message"
        return 1
    }
    
    log "Message production successful"
    
    # Additional cluster-specific tests
    if [ $replication_factor -gt 1 ]; then
        log "Running cluster-specific verification tests..."
        
        # Check topic details and replication
        log "Verifying topic replication:"
        docker exec broker1 kafka-topics.sh \
            --describe --topic $test_topic \
            --zookeeper zookeeper:2181 2>/dev/null || {
            log "WARNING: Failed to describe test topic"
        }
        
        # Verify cluster replication is complete before testing publishing
        log "Waiting for cluster replication to complete..."
        local replication_ready=0
        for i in $(seq 1 30); do  # Wait up to 30 seconds for ISR to include all replicas
            local isr_count=$(docker exec broker1 kafka-topics.sh --describe --topic $test_topic --zookeeper zookeeper:2181 2>/dev/null | grep -o "Isr: [0-9,]*" | grep -o "[0-9]" | wc -l)
            if [ "$isr_count" -eq "$replication_factor" ]; then
                log "All replicas are in-sync (ISR count: $isr_count)"
                replication_ready=1
                break
            fi
            log "Waiting for replication... (ISR count: $isr_count/$replication_factor)"
            sleep 1
        done
        
        if [ $replication_ready -eq 0 ]; then
            log "WARNING: Cluster replication not fully complete within timeout"
        fi
        
        # Test basic message production (simplified test)
        log "Testing cluster message production..."
        echo "test-cluster-msg-$(date +%s)" | docker exec -i broker1 kafka-console-producer.sh \
            --broker-list broker1:9092 \
            --topic $test_topic 2>/dev/null && {
            log "Cluster message production successful"
            pub_success=1
        } || {
            log "ERROR: Cluster message production failed"
            return 1
        }
        
        if [ $pub_success -gt 0 ]; then
            log "Cluster testing successful"
        else
            log "ERROR: Cluster testing failed"
            return 1
        fi
    fi
    
    # Clean up test topic
    docker exec broker1 kafka-topics.sh \
        --delete \
        --topic $test_topic \
        --zookeeper zookeeper:2181 2>/dev/null || {
        log "WARNING: Failed to delete test topic (this is normal for some Kafka versions)"
    }
    
    return 0
}

show_broker_info() {
    log "Kafka Broker Information:"
    
    echo ""
    echo "=== Running Containers ==="
    docker ps --filter "network=fs2-kafka-network" --format "table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}"
    
    echo ""
    echo "=== Broker Logs (last 10 lines) ==="
    for broker in broker1 broker2 broker3; do
        if docker ps --filter "name=$broker" --format "{{.Names}}" | grep -q "^$broker$"; then
            echo "--- $broker ---"
            docker logs --tail 10 $broker 2>&1 | grep -E "(started|ERROR|WARN)" || echo "No relevant log entries"
            echo ""
        fi
    done
    
    echo "=== Available Topics ==="
    docker exec broker1 kafka-topics.sh --list --zookeeper zookeeper:2181 2>/dev/null || echo "Failed to list topics"
}

main() {
    script_start "TEST-KAFKA"
    log "Starting Kafka connectivity tests..."
    
    # Test ZooKeeper
    if ! test_zookeeper; then
        error "ZooKeeper test failed"
        exit 1
    fi
    
    # Test brokers
    local failed_brokers=0
    for broker_port in "broker1:9092" "broker2:9192" "broker3:9292"; do
        local broker=$(echo $broker_port | cut -d: -f1)
        local port=$(echo $broker_port | cut -d: -f2)
        
        if docker ps --filter "name=$broker" --format "{{.Names}}" | grep -q "^$broker$"; then
            if ! test_kafka_broker $broker $port; then
                failed_brokers=$((failed_brokers + 1))
            fi
        else
            log "INFO: Broker $broker not running (single broker mode)"
        fi
    done
    
    if [ $failed_brokers -gt 0 ]; then
        error "$failed_brokers broker(s) failed tests"
        if [ "$DEBUG" = true ]; then
            show_broker_info
        fi
        exit 1
    fi
    
    # Test topic operations
    if ! test_topics; then
        error "Topic tests failed"
        if [ "$DEBUG" = true ]; then
            show_broker_info
        fi
        exit 1
    fi
    
    info "All Kafka tests passed!"
    if [ "$DEBUG" = true ]; then
        show_broker_info
    fi
    script_end "TEST-KAFKA"
}

# Run main function
main "$@"