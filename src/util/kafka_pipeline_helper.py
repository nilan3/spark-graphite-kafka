import sys

from common.kafka_pipeline import KafkaPipeline
from common.log_parsing.log_parsing_processor import LogParsingProcessor
from util import Utils


def start_log_parsing_pipeline(create_event_creators_tree):
    """
    Creates log parsing kafka pipeline and starts it

    :param create_event_creators_tree: factory method for event creators tree
    """
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        LogParsingProcessor(configuration, create_event_creators_tree(configuration))
    ).start()


def start_basic_analytics_pipeline(create_basic_analytic_processor):
    """
    Creates basic analytics kafka pipeline and starts it

    :param create_basic_analytic_processor: factory method for basic analytic processor
    """
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        create_basic_analytic_processor(configuration)
    ).start()
