import sys

from common.main_pipeline import MainPipeline
from util import Utils


def start_batch_processing_pipeline(create_processor):
    """
    Creates log parsing kafka pipeline and starts it

    :param create_event_creators_tree: factory method for event creators tree
    """
    configuration = Utils.load_config(sys.argv[:])
    MainPipeline(
        configuration,
        create_processor(configuration)
    )
