import yaml


class Configuration(object):

    def __init__(self, filename=None, dict={}):
        if (filename is not None):
            with open(filename, 'r') as stream:
                try:
                    self.__config_data = yaml.load(stream)
                except yaml.YAMLError:
                    self.__config_data = None
        else:
            self.__config_data = dict

    def property(self, name, default=None):
        if not self.__config_data:
            return default

        value = self.__config_data
        for subname in name.split('.'):
            if value and subname in value:
                value = value[subname]
            else:
                return default
        return value

    def kafka_input_options(self):
        return self.property('kafka.input.options', self.property('kafka.options'))

    def kafka_output_options(self):
        return self.property('kafka.output.options', self.property('kafka.options'))

    def kafka_bootstrap_servers(self):
        return self.property('kafka.output.options', self.property('kafka.options'))['bootstrap.servers']

    def kafka_input_topics(self):
        return self.property('kafka.input.topics', [self.property('kafka.input.topic')])

    def kafka_output_topics(self):
        return self.property('kafka.output.topics', [self.property('kafka.output.topic')])
