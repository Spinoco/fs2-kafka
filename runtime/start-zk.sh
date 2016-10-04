#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

mkdir -p $DIR/data/zookeeper

docker run -d --name="spinoco-zk" --restart=no \
-p 2181:2181/tcp \
jplock/zookeeper:3.4.8