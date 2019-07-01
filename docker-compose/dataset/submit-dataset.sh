#!/bin/bash
#$KAFKA_HOME/bin/kafka-console-producer.sh --broker-list kafka:9092 --topic query1 < /sabd/dataset/Comments_jan-apr2018.csv

java \
-agentlib:jdwp=transport=dt_socket,address=5005,server=y,suspend=n \
-cp datasource-simulator-1.0-SNAPSHOT.jar erreesse.StartSimulation Comments_jan-apr2018.csv 10000000