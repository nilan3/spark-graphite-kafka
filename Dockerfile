FROM lgi-docker.jfrog.io/odh-spark:2.3.0-1

ENV SPARK_HOME=/spark
ENV PYTHONPATH=/odh/python:$SPARK_HOME/python:$SPARK_HOME/python/build:$SPARK_HOME/python/lib/py4j-0.10.6-src.zip:$PYTHONPATH

ADD src /odh/python
RUN chmod -R 644 /odh/python
