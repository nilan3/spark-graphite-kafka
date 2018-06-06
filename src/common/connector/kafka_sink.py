from kafka import KafkaProducer

class KafkaConnector(object):
    """
    Connector for writing to kafka
    """

    @staticmethod
    def push_stats_to_kafka(options, data_frame):
        """Send the stats to kafka output topic."""
        producer = KafkaProducer(bootstrap_servers=options['bootstrap.servers'],
                                 security_protocol=options['security.protocol'],
                                 sasl_mechanism=options['sasl.mechanism'],
                                 sasl_plain_username=options['sasl.plain.username'],
                                 sasl_plain_password=options['sasl.plain.password'])

        # send each json element to output kafka topic
        for row in data_frame.toJSON().collect():
            producer.send(options['topic.output'], bytes(row))

        producer.close()
