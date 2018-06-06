import sys

from configuration import Configuration


class Utils(object):
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
