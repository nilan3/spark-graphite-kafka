# spark-graphite-kafka
Read metrics from graphite and write to kafka.

To run as a docker container:

```
docker build --rm -t kafka_ingestion_rate:1.0 .

docker run -it --net=host --rm --privileged=true --add-host=moby:127.0.0.1 --env SPARK_LOCAL_IP=127.0.0.1 -v $(pwd)/configuration:/spark/processing-conf:ro -v $(pwd)/log4j.properties:/spark/conf/log4j.properties -v $(pwd)/src:/odh/python:rw kafka_ingestion_rate:1.0 bash
```

To run spark job:

```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.1 /odh/python/applications/driver.py /spark/processing-conf/kafka_ingestion_rates.yml
```

To run unit test:

```
python /odh/python/test/unit/applications/test_kafka_ingestion.py
```

**Kafka Cheatsheet**

Console Producer & Consumer
```
kafka-console-consumer --zookeeper localhost:2181/kafka --topic  topic_name --from-beginning
kafka-console-producer --broker-list localhost:9092 --topic topic_name
kafka-topics --zookeeper localhost:2181/kafka --list
```

Topic details & Deleting topic
```
kafka-topics --describe --zookeeper localhost:2181/kafka --topic topic_name
kafka-configs --zookeeper localhost:2181/kafka --alter --entity-type topics --entity-name topic_name --add-config retention.ms=1000
kafka-topics --delete --zookeeper localhost:2181/kafka --topic topic_name
```

Check active brokers
```
zookeeper-shell localhost:2181/kafka ls /brokers/ids
```

**KafkaCat**

```
kafkacat -b localhost:9092 -L | grep topic
kafkacat -C -b localhost:9092 -o -5 -t topic_name
```
