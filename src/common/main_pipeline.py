from pyspark.sql import Row
from collections import OrderedDict

from common.abstract_pipeline import AbstractPipeline
from common.connector.graphite_source import GraphiteSource
from common.connector.kafka_sink import KafkaConnector


class MainPipeline(AbstractPipeline):
    """
    Base class for structured streaming pipeline, which read data from kafka and write result to kafka as well
    """

    def _create_custom_read_batch(self, spark):
        target = self._configuration.property("graphite.target")
        t_start = self._configuration.property("graphite.t_start")
        server = self._configuration.property("graphite.server")
        port = self._configuration.property("graphite.port")
        resp = GraphiteSource.get_data('{}:{}'.format(server, port), t_start, target)
        df_arr = []
        for series in resp:
            for point in series['datapoints']:
                record = {
                    "@timestamp": str(point[1]),
                    "value": point[0],
                    "target": series["target"]
                }
                df_arr.append(record)

        dtaRDD = spark.sparkContext.parallelize(df_arr).map(lambda x: Row(**OrderedDict(sorted(x.items()))))

        return dtaRDD.toDF()

    @staticmethod
    def __set_kafka_securing_settings(stream, options):
        return stream.option("kafka.bootstrap.servers", options["bootstrap.servers"]) \
            .option("kafka.security.protocol", options["security.protocol"]) \
            .option("kafka.sasl.mechanism", options["sasl.mechanism"])

    @staticmethod
    def __add_option_if_exists(reader, options, option):
        if option in options:
            reader.option(option, options[option])

    def _create_custom_write_batch(self, pipelines):
        streams = []
        index = 0
        for pipeline in pipelines:
            write_stream = pipeline.writeStream.format("kafka").outputMode(self._output_mode)
            options = self._configuration.property("kafka")
            streams.append(
                self.__set_kafka_securing_settings(write_stream, options)
                    .option("checkpointLocation",
                            self._configuration.property("spark.checkpointLocation") + str(index))
            )
            index += 1
        return streams
