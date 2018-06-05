from __future__ import print_function

import sys
from pyspark import SparkContext, SparkConf
from kafka import KafkaProducer, errors
import json
import requests
import datetime

from util.utils import Utils
from common.adv_analytics.kafkaUtils import KafkaConnector

def json_check(msg):
    """
    check if message is a valid json
    :param msg: string
    :return: Boolean
    """
    try:
        json.loads(msg[1])
    except (ValueError, TypeError) as e:
        return False
    return True


def form_json(msg):
    """
    Transform string to dict
    TN_CHU_PROD_CF,EPG_CF_RENG_REP_01,uc-l-p-obo00025
    :param msg: string
    :return: dict
    """
    [tn, epg, vm] = msg.split(',')
    return {
        "tenant": tn,
        "epg": epg,
        "virtual_machine": vm
    }


def prepare_doc(msg):
    """
    Add timestamp
    :param msg: input stream message
    :return: document for export to ES
    """
    doc = msg.copy()
    doc["timestamp"] = str(datetime.datetime.now())
    return doc


def flatten(msg):
    """
    flatten nested json into lists
    :param msg:
    :return:
    """
    res = []
    for tn in msg[1]:
        tenant = tn['tenant']
        for epg in tn['epg']:
            for endpoint in tn['epg'][epg]:
                res.append('{},{},{}'.format(tenant, epg, endpoint['virtual_machine']))
    return res


def export_to_hdfs(input_stream):
    """

    :param input_stream:
    :return:
    """
    output = input_stream \
        .map(lambda x: ('hdfs', [json.loads(x[1])])) \
        .reduceByKey(lambda x, y: x + y) \
        .flatMap(lambda msg: flatten(msg))

    return output


def hdfs_sink(rdd):
    """
    Save rdd to HDFS
    :param rdd:
    :return:
    """
    if not len(rdd.take(1)) == 0:
        rdd.repartition(1).saveAsTextFile(config.property('hdfs.outputPath'))


def read_data(kafka_stream):
    """
    Take input stream and encode all topic messages into json format.
    Ignore messages that do not contained the required fields.
    :param kafkaStream: Input stream
    :return: filtered input stream
    """
    rdd_stream = kafka_stream \
        .filter(json_check) \
        .map(lambda x: json.loads(x[1])) \
        .map(lambda msg: prepare_doc(msg))
    return rdd_stream


def es_sink(doc):
    """
    Export to ES
    :param doc: message
    """
    headers = {
        "content-type": "application/json"
    }
    url = "http://{}/{}/obo/{}".format(config.property('es.host'), config.property('es.index'), doc["tenant"])
    try:
        requests.request("PUT", url, data=json.dumps(doc), headers=headers)
    except requests.ConnectionError:
        pass


def send_partition_es(iter):
    """
    Sink to ES
    :param iter:
    :return:
    """
    for record in iter:
        es_sink(record)


def kafka_sink(topic, msg, producer):
    """
    Send json message into kafka topic. Avoid async operation to guarantee
    msg is delivered.
    """
    try:
        producer.send(topic, json.dumps(msg).encode('utf-8')).get(timeout=30)
    except errors.KafkaTimeoutError:
        pass


def send_partition_kafka(iter):
    """
    Sink to kafka
    :param iter:
    :return:
    """
    topic = config.property('kafka.topicOutput')
    producer = KafkaProducer(bootstrap_servers = config.property('kafka.bootstrapServers').split(','))
    for record in iter:
        kafka_sink(topic, record, producer)
    producer.flush()


if __name__ == "__main__":
    config = Utils.load_config(sys.argv[1])
    myConf = SparkConf().setAppName(config.property('spark.appName')).setMaster(config.property('spark.master')).set(
        "spark.hadoop.validateOutputSpecs", "false")
    sc = SparkContext(conf=myConf)
    sc.setLogLevel("WARN")
    ssc = KafkaConnector.create_spark_context(config, sc)

    input_stream = KafkaConnector(config).create_kafka_stream(ssc)
    hdfs_stream = export_to_hdfs(input_stream)
    hdfs_stream.cache()
    hdfs_stream.foreachRDD(lambda rdd: hdfs_sink(rdd))
    hdfs_stream.map(lambda msg: form_json(msg)).foreachRDD(lambda rdd: rdd.foreachPartition(send_partition_kafka))
    es_stream = read_data(input_stream)
    sink = es_stream.foreachRDD(lambda rdd: rdd.foreachPartition(send_partition_es))
    ssc.start()
    ssc.awaitTermination()
