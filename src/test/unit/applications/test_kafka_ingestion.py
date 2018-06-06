import unittest

from test.unit.common.test_pipeline_helper import start_test_pipeline
from applications.driver import create_processor

class TestMethod(unittest.TestCase):
    """
    Example Test
    """
    def test_sum(self):
        """
        Test for sum aggregation
        :return:
        """
        config_path = "/odh/python/test/unit/resources/test_configurations.yml"
        input = [
            {
                "datapoints": [
                    [38.88631905155365, 1528294230],
                    [None, 1528294245],
                    [23.585744748134786, 1528294260],
                    [None, 1528294275],
                    [4490.293497634195, 1528294290],
                    [None, 1528294305],
                    [8034.7691023577045, 1528294320],
                    [None, 1528294335],
                    [9710.930471058657, 1528294350],
                    [None, 1528294365],
                    [9398.345584760124, 1528294380],
                    [None, 1528294395],
                    [6815.876540085096, 1528294410],
                    [None, 1528294425],
                    [4469.037611843548, 1528294440],
                    [None, 1528294455],
                    [3271.2549398688293, 1528294470],
                    [None, 1528294485],
                    [2505.116751199644, 1528294500],
                    [None, 1528294515],
                    [None, 1528294530]
                ],
                "target": "odhecx_in_eosdtv_cl_prd_heapp_traxis_frontend_log_gen_v1",
                "tags": {
                    "aggregatedBy": "sum",
                    "name": "sumSeries(servers.*.KafkaBrokerTopicMetrics.{odhecx_in_eosdtv_cl_prd_heapp_traxis_frontend_log_gen_v1}.MessagesInPerSec.OneMinuteRate)"
                }
            }
        ]
        output = [
            {
                "@timestamp": "1528294530",
                "target": "odhecx_in_eosdtv_cl_prd_heapp_traxis_frontend_log_gen_v1",
                "value": 48758.09656260748
            }
        ]
        res = start_test_pipeline(create_processor, input, config_path)
        self.assertEqual(res, output)

if __name__ == '__main__':
    unittest.main()

