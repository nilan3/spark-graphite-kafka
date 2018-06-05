import sys
from common.main_pipeline import KafkaPipeline
from util.utils import Utils
from pyspark.sql.functions import from_json, lit, col, struct, udf
from pyspark.sql.types import StringType, DoubleType, StructType, StructField

class VmCloudmapCorrelation(object):
    """
    Tie VM-EPG-Tenant mapping with VROPS VM metrics stream
    """

    def __init__(self, configuration, schema):

        self.__configuration = configuration
        self._schema = schema
        self.kafka_output = configuration.property("kafka.topics.output")

    def create(self, read_stream):
        """
        Create final stream to output to kafka
        :param read_stream:
        :return: Kafka stream
        """
        spark = read_stream.sql_ctx
        schema = StructType([
            StructField("tenant", StringType(), True),
            StructField("epg", StringType(), True),
            StructField("vm", StringType(), True)])
        cloudmap_df = spark \
            .read.csv(self.__configuration.property("analytics.hdfsFilePath"), header=False, schema=schema)
        json_stream = read_stream \
            .select(from_json(read_stream["value"].cast("string"), self._schema).alias("json")) \
            .select("json.*")

        return [self._convert_to_kafka_structure(dataframe) for dataframe in self._process_pipeline(json_stream, cloudmap_df)]

    def _convert_to_kafka_structure(self, dataframe):
        """
        Convert to json schema and add topic name as output
        :param dataframe:
        :return: output stream
        """
        return dataframe \
            .selectExpr("to_json(struct(*)) AS value") \
            .withColumn("topic", lit(self.kafka_output))

    def _update_metric_name(self, stream):
        """
        Include tenant.epg.vm in carbon metric path
        :param stream: input filtered stream
        :return: finalised output stream
        """
        def udf_modify_metric_name(row):
            metric_name = row[4]
            tenant = row[6].split('_')[1]
            epg = row[7]
            vm = row[8]

            new_metric_name = metric_name\
                .replace('.name.' + vm, '')\
                .replace('group', '{}.epg.{}.vm.{}'.format(tenant, epg, vm))

            return new_metric_name

        update_metric_name = udf(lambda row: udf_modify_metric_name(row), StringType())
        final = stream \
            .withColumn("metric_name", update_metric_name(struct([stream[x] for x in stream.columns])))

        return final

    def _process_pipeline(self, stream, mapping):
        """
        Pipeline method
        :param json_stream: kafka stream reader
        :return: list of streams
        """
        filtered = stream.join(mapping, stream.name == mapping.vm, 'leftouter').where(col("vm").isNotNull())
        res = self._update_metric_name(filtered)

        return [res]

    @staticmethod
    def create_schema():
        """
        Schema for input stream.
        """
        return StructType([
            StructField("res_kind", StringType()),
            StructField("group", StringType()),
            StructField("name", StringType()),
            StructField("@timestamp", StringType()),
            StructField("metric_name", StringType()),
            StructField("value", DoubleType())
        ])

def create_processor(configuration):
    """
    Build processor using configurations and schema.
    :param configuration:
    :return:
    """
    return VmCloudmapCorrelation(configuration, VmCloudmapCorrelation.create_schema())


if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        create_processor(configuration)
    ).start()
