from common.test_pipeline import TestPipeline
from util import Utils


def start_test_pipeline(create_processor, input_data, config_path):
    """
    Creates test pipeline and get results
    """
    configuration = Utils.load_config(config_path)
    batch = TestPipeline(
        configuration,
        create_processor(configuration),
        input_data
    )
    return batch._write_streams
