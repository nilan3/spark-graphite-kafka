"""
Module for abstract spark pipeline
"""
from abc import ABCMeta, abstractmethod
from pyspark.sql import SparkSession


class AbstractPipeline(object):
    """
    Base class for structured streaming pipeline
    """
    __metaclass__ = ABCMeta

    def __init__(self, configuration, processor):
        """
        Creates instance
        :param configuration: yml configuration
        :param processor: processor which creates transformations
        """

        self._configuration = configuration
        self._output_mode = configuration.property("spark.outputMode", "append")
        self.spark = self.__create_spark_session()
        read_stream = self._create_custom_read_batch(self.spark)
        pipelines = processor.create(read_stream, self.spark)
        self._write_streams = self.__create_write_batch(pipelines)

    def __create_spark_session(self):
        options = self._configuration.property("spark")
        result = SparkSession.builder \
            .appName(self._configuration.property("spark.appName")) \
            .master(self._configuration.property("spark.master")) \
            .config("spark.sql.session.timeZone", "UTC")
        self.__add_config_if_exists(result, "spark.sql.shuffle.partitions", options, "shuffle.partitions")
        return result.getOrCreate()

    @staticmethod
    def __add_config_if_exists(config, key, options, option):
        if option in options:
            config.config(key, options[option])

    @abstractmethod
    def _create_custom_read_batch(self, spark):
        """
        Abstract fabric method for custom reader
        :param spark: spark session
        :return: custom read batch
        """

    @abstractmethod
    def _create_custom_write_batch(self, batch):
        """
        Abstract fabric method for custom writer
        :param spark: spark session
        :return: custom read batch
        """

    def __create_write_batch(self, pipelines):
        if self._configuration.property("spark.consoleWriter"):
            return self.__create_console_write_batch(pipelines)
        else:
            return self._create_custom_write_batch(pipelines)

    def __create_console_write_batch(self, df):
        return df.show(347, False)
