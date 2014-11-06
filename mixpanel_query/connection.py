"""
The class(es) in this module contain logic to make http
requests to the Mixpanel API.
"""
import hashlib
import json
import time
import urllib
import urllib2


class Connection(object):
    """
    The `Connection` object's sole responsibility is to format, send to
    and parse http responses from the Mixpanel API.
    """
    ENDPOINT = 'http://mixpanel.com/api'
    DATA_ENDPOINT = 'https://data.mixpanel.com/api'
    VERSION = '2.0'

    def __init__(self, client):
        self.client = client

    def request(self, method_name, params, response_format='json'):
        """
        Make a request to Mixpanel query endpoints and return the
        parsed response.
        """
        request = self.raw_request(self.ENDPOINT, method_name, params, response_format)
        data = request.read()
        return json.loads(data)

    def raw_request(self, base_url, method_name, params, response_format):
        """
        Make a request to the Mixpanel API and return a raw urllib2 file-like
        response object.
        """
        params['api_key'] = self.client.api_key
        params['expire'] = int(time.time()) + 600   # Grant this request 10 minutes.
        params['format'] = response_format
        # Getting rid of the None params
        params = self.check_params(params)

        # Creating signature
        if 'sig' in params:
            del params['sig']
        params['sig'] = self.hash_args(params, self.client.api_secret)

        request_url = '{base_url}/{version}/{method_name}/?{encoded_params}'.format(
            base_url=base_url,
            version=self.VERSION,
            method_name=method_name,
            encoded_params=self.unicode_urlencode(params)
        )
        return urllib2.urlopen(request_url, timeout=120)

    def unicode_urlencode(self, params):
        """
        Convert lists to JSON encoded strings, and correctly handle any
        unicode URL parameters.
        """
        if isinstance(params, dict):
            params = params.items()
        for i, param in enumerate(params):
            if isinstance(param[1], list):
                params[i] = (param[0], json.dumps(param[1]),)

        return urllib.urlencode(
            [(k, isinstance(v, unicode) and v.encode('utf-8') or v) for k, v in params]
        )

    def hash_args(self, args, secret=None):
        """
        Hashes arguments by joining key=value pairs, appending the api_secret, and
        then taking the MD5 hex digest.
        """
        for a in args:
            if isinstance(args[a], list):
                args[a] = json.dumps(args[a])

        args_joined = ''
        for a in sorted(args.keys()):
            if isinstance(a, unicode):
                args_joined += a.encode('utf-8')
            else:
                args_joined += str(a)

            args_joined += '='

            if isinstance(args[a], unicode):
                args_joined += args[a].encode('utf-8')
            else:
                args_joined += str(args[a])

        hash = hashlib.md5(args_joined)

        if secret:
            hash.update(secret)
        elif self.client.api_secret:
            hash.update(self.client.api_secret)
        return hash.hexdigest()

    def check_params(self, params):
        copyParams = params.copy()
        for key in copyParams.iterkeys():
            if not copyParams[key]:
                del params[key]

        return params
