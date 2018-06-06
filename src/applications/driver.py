from time import time
import json

from util.pipeline_helper import start_batch_processing_pipeline
from pyspark.sql.functions import lit, col, sum


class KafkaIngestionRate(object):
    """
    Find top 20 topics with the greatest ingestion rates and
    """

    def __init__(self, configuration):

        self.kafka_output = configuration.property("kafka.topics.output")
        self.topic_prefix = configuration.property("graphite.topicPrefix")

    def create(self, read_batch, spark):
        """
        Create final stream to output to kafka
        :param read_stream:
        :return: Kafka stream
        """
        filtered = read_batch \
            .filter(col('value').isNotNull()) \
            .sort("value", ascending=False)

        return self._process_pipeline(filtered, spark)

    def _process_pipeline(self, df, spark):
        """
        Pipeline method
        :param json_stream: kafka stream reader
        :return: list of streams
        """

        df.cache()

        ts = str(int(round(time())))
        total = json.loads(df.groupBy().agg(sum("value")).toJSON().collect()[0])["sum(value)"]
        target = self.topic_prefix + "*"

        new_row = spark.createDataFrame([[ts, target, total]], ["@timestamp", "target", "message_count"])

        final = new_row.union(df).limit(21)

        return final


def create_processor(configuration):
    """
    Build processor using configurations and schema.
    :param configuration:
    :return:
    """
    return KafkaIngestionRate(configuration)


if __name__ == "__main__":
    start_batch_processing_pipeline(create_processor)
