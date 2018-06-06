import json
import re
import sys
from kafka import KafkaConsumer
from requests import get

class GraphiteSource(object):
    """
    Takes an input stream grouped by key (hostname) adn returns a flattened stream
    with value holding component metrics tied with vm metrics.
    """
    @staticmethod
    def query_graphite(endpoint, t_start, target, topic_list):
        """
        Fetch data points for list of topics
        :param endpoint:
        :param t_start:
        :param target:
        :return:
        """
        url = "http://%s/render?from=%s&target=%s&format=json&tz=UTC" % (endpoint, t_start, target)
        sub_url = url.replace('topics', topic_list)
        retries = 3
        for attempt in list(range(retries)):
            # Query
            resp = get(sub_url)

            if not (resp.status_code < 200 or resp.status_code > 299):
                data = resp.json() if not isinstance(resp, str) else json.loads(resp)
                break
            else:
                print('failed to fetch data from graphite, attempt: '+ str(attempt+1))
                data = []

        return data

    @staticmethod
    def format_data(series, agg):
        """

        :param series:
        :return:
        """
        sum = 0.0
        count = 0
        ts = []
        for point in series['datapoints']:
            if point[0] != None:
                sum += point[0]
                count += 1
            ts.append(point[1])

        if agg == 'avg':
            value = sum/count
        elif agg == 'sum':
            value = sum

        record = {
            "@timestamp": str(max(ts)),
            "value": value,
            "target": series["target"]
        }
        return record


    @staticmethod
    def get_data(kafka_options, graphite_options, spark):
        """
        Build url - get response
        :return: dict list
        """
        log4jLogger = spark.sparkContext._jvm.org.apache.log4j
        LOGGER = log4jLogger.LogManager.getLogger(__name__)
        retries = 3
        for attempt in list(range(retries)):
            try:
                cons = KafkaConsumer(
                    bootstrap_servers=kafka_options['bootstrap.servers'], security_protocol=kafka_options['security.protocol'],
                    sasl_mechanism=kafka_options['sasl.mechanism'], sasl_plain_username=kafka_options['sasl.plain.username'],
                    sasl_plain_password=kafka_options['sasl.plain.password'])
                LOGGER.info("Successfully connected to Kafka")
                break
            except Exception:
                LOGGER.error("failed to connect to kafka, attempt: "+ str(attempt+1))
                if attempt == retries-1:
                    sys.exit(1)

        topics = list(cons.topics())
        r = re.compile("{}.*".format(graphite_options['topicPrefix']))
        filtered = list(filter(r.match, topics))
        length = 10
        filtered_groups = [','.join(filtered[i:i+length]) for i  in range(0, len(filtered), length)]
        rdd = spark\
            .sparkContext.parallelize(filtered_groups)

        return rdd \
            .flatMap(
            lambda topic_str: GraphiteSource.query_graphite(graphite_options['server'], graphite_options['tStart'],
                                                            graphite_options['target'], topic_str)) \
            .map(lambda x: GraphiteSource.format_data(x, graphite_options['aggregation']))

    @staticmethod
    def test_sample_data(sample_data, graphite_options, spark):
        """
        Build url - get response
        :return: dict list
        """
        rdd = spark \
            .sparkContext.parallelize(sample_data)

        return rdd \
            .map(lambda x: GraphiteSource.format_data(x, graphite_options['aggregation']))
