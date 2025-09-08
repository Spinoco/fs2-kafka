#!/bin/bash

# Stop Kafka and ZooKeeper containers for fs2-kafka testing
# Usage: ./stop-kafka.sh [--debug]

# Remove set -e to allow cleanup to continue even if some commands fail

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

NETWORK_NAME="fs2-kafka-network"

stop_containers() {
    log "Stopping Kafka and ZooKeeper containers..."
    
    # Force stop and remove containers - more aggressive approach
    for container in broker3 broker2 broker1 zookeeper; do
        log "Force stopping and removing $container..."
        docker kill $container 2>/dev/null || true
        docker rm -f $container 2>/dev/null || true
    done
    
    # Also try to stop any containers using our network
    local containers_in_network=$(docker ps --filter "network=$NETWORK_NAME" --format "{{.Names}}" 2>/dev/null || true)
    if [ -n "$containers_in_network" ]; then
        log "Stopping containers in $NETWORK_NAME network: $containers_in_network"
        echo "$containers_in_network" | xargs -r docker kill 2>/dev/null || true
        echo "$containers_in_network" | xargs -r docker rm -f 2>/dev/null || true
    fi
}

cleanup_network() {
    log "Cleaning up Docker network: $NETWORK_NAME"
    
    # Wait a moment for containers to fully stop
    sleep 2
    
    # Force remove network
    docker network rm $NETWORK_NAME 2>/dev/null || true
    
    # If that fails, try to disconnect any remaining containers first
    local connected_containers=$(docker network inspect $NETWORK_NAME --format "{{range .Containers}}{{.Name}} {{end}}" 2>/dev/null || true)
    if [ -n "$connected_containers" ]; then
        log "Disconnecting containers from network: $connected_containers"
        for container in $connected_containers; do
            docker network disconnect $NETWORK_NAME $container 2>/dev/null || true
        done
        # Try removing network again
        docker network rm $NETWORK_NAME 2>/dev/null || true
    fi
}

cleanup_orphaned() {
    log "Cleaning up any orphaned containers..."
    
    # Kill any containers that might be using our ports
    for port in 2181 9092 9192 9292; do
        container_id=$(docker ps --filter "publish=$port" --format "{{.ID}}" | head -n 1)
        if [ -n "$container_id" ]; then
            log "Killing orphaned container using port $port: $container_id"
            docker kill $container_id || true
            docker rm $container_id || true
        fi
    done
    
    # Clean up any containers with fs2-kafka in the name
    orphaned=$(docker ps -a --filter "name=fs2-kafka" --format "{{.ID}}" | head -10)
    if [ -n "$orphaned" ]; then
        log "Cleaning up fs2-kafka related containers..."
        echo "$orphaned" | xargs -r docker rm -f
    fi
}

show_status() {
    log "Current Docker status:"
    
    echo ""
    echo "=== Running Containers ==="
    docker ps --format "table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}"
    
    echo ""
    echo "=== Networks ==="
    docker network ls --format "table {{.Name}}\t{{.Driver}}\t{{.Scope}}"
    
    echo ""
    echo "=== Port Usage ==="
    for port in 2181 9092 9192 9292; do
        if lsof -i :$port >/dev/null 2>&1; then
            echo "Port $port: IN USE"
        else
            echo "Port $port: free"
        fi
    done
}

main() {
    script_start "STOP-KAFKA"
    log "Stopping fs2-kafka test environment..."
    
    stop_containers
    cleanup_network
    cleanup_orphaned
    
    info "Cleanup complete!"
    if [ "$DEBUG" = true ]; then
        show_status
    fi
    script_end "STOP-KAFKA"
}

# Run main function
main "$@"