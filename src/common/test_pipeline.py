from pyspark.sql import Row
from collections import OrderedDict

from common.abstract_pipeline import AbstractPipeline
from common.connector.graphite_source import GraphiteSource


class TestPipeline(AbstractPipeline):
    """
    Base class for structured streaming pipeline, which read data from file and write to memory
    """

    def __init__(self, configuration, processor, input_data):
        self.__input_data = input_data
        super(TestPipeline, self).__init__(configuration, processor)

    def _create_custom_read_batch(self, spark):
        rdd = GraphiteSource.test_sample_data(self.__input_data, self._configuration.property("graphite"), spark)
        rdd.cache()

        data_rdd = rdd.map(lambda x: Row(**OrderedDict(sorted(x.items()))))

        return spark.createDataFrame(data_rdd)

    def _create_custom_write_batch(self, df):
        return map(lambda row: row.asDict(), df.collect())
