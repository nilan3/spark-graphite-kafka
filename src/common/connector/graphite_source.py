import json
from requests import get

class GraphiteSource(object):
    """
    Takes an input stream grouped by key (hostname) adn returns a flattened stream
    with value holding component metrics tied with vm metrics.
    """


    def get_data(endpoint, t_start, target):
        """
        Build url - get response
        :return: dict list
        """
        url = "http://%s/render?from=%s&target=%s&format=json&tz=UTC" % (endpoint, t_start, target)
        # Query
        resp = get(url)

        if not (resp.status_code < 200 or resp.status_code > 299):
            data = resp.json() if not isinstance(resp, str) else json.loads(resp)
        else:
            data = []

        return data
