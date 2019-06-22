#!/bin/bash
$KAFKA_HOME/bin/kafka-console-producer.sh --broker-list kafka:9092 --topic query1 < /sabd/dataset/Comments_jan-apr2018.csv