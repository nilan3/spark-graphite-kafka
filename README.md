# spark-graphite-kafka
Read metrics from graphite and write to kafka.

To run as a docker container:

```
docker build --rm -t kafka_ingestion_rate:1.0 .

docker run -it --net=host --rm --privileged=true --add-host=moby:127.0.0.1 --env SPARK_LOCAL_IP=127.0.0.1 -v $pwd/configuration:/spark/processing-conf:ro -v $pwd/log4j.properties:/spark/conf/log4j.properties -v $pwd/src:/odh/python:rw kafka_ingestion_rate:1.0 bash
```

To run spark job:

```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.1 /odh/python/applications/driver.py /spark/processing-conf/kafka_ingestion_rates.yml
```

To run unit test:

```
python /odh/python/test/unit/applications/test_kafka_ingestion.py
```
