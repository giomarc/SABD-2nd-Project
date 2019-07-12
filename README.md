# SABD-2nd-Project
Repo for SABD course project year 2018/2019.

Design and implement a data stream processing system for compute queries on dataset composed by comments from an online newspaper

Environment
------
![environment](https://raw.githubusercontent.com/loplace/SABD-2nd-Project/master/environment.png?token=AD3JGRQ2EA2ID7ELZIXBSWC5GBY44)

Environment is composed by 9 docker containers:
* hdfs datanode
* hdfs namenode
* alluxio master
* alluxio worker
* zookeeper
* kafka broker
* kafka producer
* flink job manager
* flink task manager
 
- HDSF is used to persist flink operator's state
- Alluxio is used as low latency middleware layer for persisting state between HDFS and Flink
- Zookeeper for high availability for Kafka
- Kafka as ingestion layer for Flink
- Flink as data stream processing framework


Starting the environment
------
The environment can be managed by docker-compose. To start, type the following command in docker-compose folder
```console
docker-compose up -d --build
```

All containers start automatically,just wait a few minutes.

### Phase 1: Data ingestion

Attach a shell to kafka-producer container typing following command:
```console
docker-exec -it kafka-producer bash
```
then
```console
/sabd/dataset/submit-dataset.sh
```
Usage of submit-dataset.sh
```console
Usage: submit-dataset.sh -f <filename> -s <speedfactor>

-f : filename of dataset to inject
-s : speed factor for accelerate the ingestion
     1:   real time
     10:  10 times faster
     100: 100 times faster
```

For example 
```console
submit-dataset.sh -f Comments_jan-apr2018.csv -s 10000
```
to start the ingestion of dataset in kafka at 10000x faster replay speed


For monitoring:

- HDFS dashboard is available at http://localhost:9870
- Alluxio dashboard is available at http://localhost:19999
- Flink dashboard is available at http://localhost:8081

### Phase 2: Submit Flink Job

Attach a shell to jobmanager container typing following command:
```console
docker-exec -it jobmanager bash
```
then
```console
/sabd/jar/submit-jar.sh
```
Usage of submit-jar.sh
```console
Usage: submit-jar.sh -q <queryname>

-q : name of query [query1|query2|query3]
```

For example 
```console
submit-jar.sh -q query1
```
to start the Flink job for query1.

## Owners

Ronci Federico - Computer Science Engineering - University of Rome Tor Vergata

Schiazza Antonio - Computer Science Engineering - University of Rome Tor Vergata
