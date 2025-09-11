#!/bin/bash

# Common utility functions for fs2-kafka scripts
# Usage: source ./common-utils.sh

# Format timestamp for consistent logging
timestamp() {
    date '+%Y-%m-%d %H:%M:%S'
}

# Debug logging - only shown when DEBUG=true
log() {
    if [ "${DEBUG:-false}" = true ]; then
        echo "[$(timestamp)] DEBUG: $*"
    fi
}

# Info logging - always shown with timestamp
info() {
    echo "[$(timestamp)] $*"
}

# Error logging - always shown with timestamp and ERROR prefix
error() {
    echo "[$(timestamp)] ERROR: $*" >&2
}

# Script start/end markers
script_start() {
    local script_name=$1
    info "=== $script_name STARTED ==="
}

script_end() {
    local script_name=$1
    info "=== $script_name COMPLETED ==="
}