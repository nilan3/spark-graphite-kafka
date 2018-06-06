from pyspark.sql import Row
from collections import OrderedDict

from common.abstract_pipeline import AbstractPipeline
from common.connector.graphite_source import GraphiteSource
from common.connector.kafka_sink import KafkaConnector


class MainPipeline(AbstractPipeline):
    """
    Base class for batch processing pipeline, which read data from graphite and write result to kafka as well
    """

    def _create_custom_read_batch(self, spark):
        resp = GraphiteSource.get_data(self._configuration.property("kafka"), self._configuration.property("graphite"), spark)
        resp.cache()

        data_rdd = resp.map(lambda x: Row(**OrderedDict(sorted(x.items()))))

        return spark.createDataFrame(data_rdd)

    @staticmethod
    def __set_kafka_securing_settings(stream, options):
        return stream.option("kafka.bootstrap.servers", options["bootstrap.servers"]) \
            .option("kafka.security.protocol", options["security.protocol"]) \
            .option("kafka.sasl.mechanism", options["sasl.mechanism"])

    @staticmethod
    def __add_option_if_exists(reader, options, option):
        if option in options:
            reader.option(option, options[option])

    def _create_custom_write_batch(self, batch):
        return KafkaConnector.push_stats_to_kafka(self._configuration.property("kafka"), batch)
