import json
from requests import get

class GraphiteSource(object):
    """
    Takes an input stream grouped by key (hostname) adn returns a flattened stream
    with value holding component metrics tied with vm metrics.
    """

    @staticmethod
    def getData(endpoint, t_start, t_end, target, data_format):
        """
        Build url - get response - convert to DataFrame - drop NAs - return DF
        :return: pandas.DataFrame
        """
        url = "http://%s/render?from=%s&until=%s&target=%s&format=%s&tz=UTC" % (
            endpoint, t_start, t_end, target, data_format)
        # Query
        try:
            logger.debug(url)
            resp = get(url)
        except Exception as e:
            logger.error(e)

        if isinstance(resp, str):
            logger.error('Unable to download data from {}'.format(url))

        # Check the response code
        if not isinstance(resp, str) and (resp.status_code < 200 or resp.status_code > 299):
            logger.error("Graphite - impossible to get the data, status code = %s, reason = %s" % (
                resp.status_code, resp.reason))
            resp = None
        else:
            resp = resp.json() if not isinstance(resp, str) else json.loads(resp)
