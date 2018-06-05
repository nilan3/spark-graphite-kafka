import sys
from datetime import datetime
from datetime import timedelta

from configuration import Configuration

default_date_format = "%Y-%m-%d %H:%M:%S,%f"


class Utils(object):
    @staticmethod
    def parse_datetime(text, date_format=default_date_format):
        return datetime.strptime(text, date_format)

    @staticmethod
    def format_datetime(date, date_format=default_date_format):
        return date.strftime(date_format)

    @staticmethod
    def datetime_add_seconds(date, seconds=0.0):
        return date + timedelta(0, seconds)

    @staticmethod
    def load_file(filename):
        with open(filename, "r") as resource:
            return resource.read()

    @staticmethod
    def load_config(args):
        config_file = args if isinstance(args, basestring) else args[1] if len(args) > 1 else None
        if config_file:
            config = Configuration(config_file)
            if config:
                return config

        print("usage: script.py <config.yaml>")
        sys.exit(1)

    @staticmethod
    def get_output_topic(configuration, component_name):
        return configuration.property("kafka.topics.outputs." + component_name) if configuration is not None else None
