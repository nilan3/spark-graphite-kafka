import yaml


class Configuration(object):

    def __init__(self, filename=None, dict={}):
        print(filename)
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

    def graphite_server(self):
        return self.property('graphite.server')

    def graphite_port(self):
        return self.property('graphite.port')

    def kafka_bootstrap_servers(self):
        return self.property('kafka.output')['bootstrap.servers']

    def kafka_output_topics(self):
        return self.property('kafka.output.topics')
