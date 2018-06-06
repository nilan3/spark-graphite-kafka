from util.pipeline_helper import start_batch_processing_pipeline
from test.unit.common.test_pipeline_helper import TestPipeline
from pyspark.sql.functions import lit, col, struct, udf


class KafkaIngestionRate(object):
    """
    Find top 20 topics with the greatest ingestion rates and
    """

    def __init__(self, configuration):

        self.kafka_output = configuration.property("kafka.topics.output")

    def create(self, read_batch):
        """
        Create final stream to output to kafka
        :param read_stream:
        :return: Kafka stream
        """
        return self._process_pipeline(read_batch)

    def _process_pipeline(self, df):
        """
        Pipeline method
        :param json_stream: kafka stream reader
        :return: list of streams
        """
        filtered = df \
            .filter(col('value').isNotNull()) \
            .sort("value", ascending=False)

        return filtered

def create_processor(configuration):
    """
    Build processor using configurations and schema.
    :param configuration:
    :return:
    """
    return KafkaIngestionRate(configuration)


if __name__ == "__main__":
    start_batch_processing_pipeline(create_processor)
