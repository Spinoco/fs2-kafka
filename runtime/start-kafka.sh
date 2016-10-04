#!/usr/bin/env bash


DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

docker run -d --name="spinoco-fs2-kafka" --restart=no --link spinoco-zk:spinoco-zk \
-e "KAFKA_ADVERTISED_HOST_NAME=127.0.0.1" \
-e "KAFKA_ADVERTISED_PORT=9092" \
-e "KAFKA_ZOOKEEPER_CONNECT=spinoco-zk:2181" \
-e "KAFKA_CREATE_TOPICS=test:1:1" \
-p 9092:9092/tcp \
wurstmeister/kafka:0.10.0.0