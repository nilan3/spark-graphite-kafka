import sys

from common.main_pipeline import KafkaPipeline
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
