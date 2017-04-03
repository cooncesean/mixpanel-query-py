"""
The class(es) in this module contain logic to make http
requests to the Mixpanel API.
"""
import json

import six

from six.moves.urllib import request as url_request

__all__ = ('Connection',)

class Connection(object):
    """
    The `Connection` object's sole responsibility is to format, send to
    and parse http responses from the Mixpanel API.
    """
    ENDPOINT = 'https://mixpanel.com/api'
    DATA_ENDPOINT = 'https://data.mixpanel.com/api'
    VERSION = '2.0'
    DEFAULT_TIMEOUT = 120

    def __init__(self, client):
        self.client = client

    def request(self, method_name, params, response_format='json'):
        """
        Make a request to Mixpanel query endpoints and return the
        parsed response.
        """
        request = self.raw_request(self.ENDPOINT, method_name, params, response_format)
        data = request.read()
        return json.loads(data.decode('utf-8'))

    def raw_request(self, base_url, method_name, params, response_format):
        """
        Make a request to the Mixpanel API and return a raw urllib2/url.request file-like
        response object.
        """
        params['format'] = response_format
        # Getting rid of the None params
        params = self.check_params(params)
        url_without_params = '{base_url}/{version}/{method_name}/'.format(
            base_url=base_url,
            version=self.VERSION,
            method_name=method_name,
        )
        request_obj = self.client.auth.authenticate(url_without_params, params)
        effective_timeout = self.DEFAULT_TIMEOUT if self.client.timeout is None else self.client.timeout
        return url_request.urlopen(request_obj, timeout=effective_timeout)

    def check_params(self, params):
        copyParams = params.copy()
        for key in six.iterkeys(copyParams):
            if not copyParams[key]:
                del params[key]

        return params
