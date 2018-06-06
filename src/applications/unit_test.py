import unittest

from common.test_pipeline import TestPipeline

class TestMethod(unittest.TestCase):
    """
    Example Test
    """
    def test_sum(self):
        """
        Test for sum aggregation
        :return:
        """
        res = TestPipeline()

if __name__ == '__main__':
    unittest.main()
