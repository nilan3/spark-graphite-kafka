import sys

from common.test_pipeline import TestPipeline
from util import Utils


def start_test_pipeline(create_processor, input_data, config_path):
    """
    Creates log parsing kafka pipeline and starts it

    :param create_event_creators_tree: factory method for event creators tree
    """
    configuration = Utils.load_config(config_path)
    batch = TestPipeline(
        configuration,
        create_processor(configuration),
        input_data
    )
    return batch._write_streams
