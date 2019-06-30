#!/bin/bash
hdfs namenode -format
$HADOOP_HOME/sbin/start-dfs.sh

sleep 30

hdfs dfs -chmod -R 777 /