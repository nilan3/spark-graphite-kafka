FROM smartislav/spark:2.3.0-jdk8u121-mesos1.5.0

RUN apt-get -y update \
 && apt-get -y install --no-install-recommends python-pip python-setuptools python-wheel \
 && apt-get clean

RUN pip install PyYAML==3.12 \
 && pip install kafka==1.3.3

ENV SPARK_HOME=/spark
ENV PYTHONPATH=/odh/python:$SPARK_HOME/python:$SPARK_HOME/python/build:$SPARK_HOME/python/lib/py4j-0.10.6-src.zip:$PYTHONPATH

ADD src /odh/python
RUN chmod -R 644 /odh/python
