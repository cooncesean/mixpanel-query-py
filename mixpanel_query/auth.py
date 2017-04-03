"""
The classes in this module contain the logic to authenticate an http request to
the Mixpanel API
"""
import base64
import hashlib
import json
import time

import six
from mixpanel_query.utils import _tobytes, _totext, _unicode_urlencode

from six.moves.urllib import request as url_request


class SignatureAuth(object):
    """
    Signature-based authentication uses your api secret to create and md5 hash
    of your request's parameters for verification by mixpanel

    This method is deprecated, but mixpanel currently has no plans to remove
    support for this method of authentication.

    Please see https://mixpanel.com/help/reference/data-export-api#authentication
    for more details.
    """
    DEFAULT_EXPIRATION = 600  # expire requests after 10 minutes

    def __init__(self, client):
        self.client = client

    def _hash_args(self, args, secret=None):
        """
        Hashes arguments by joining key=value pairs, appending the api_secret, and
        then taking the MD5 hex digest.
        """
        for arg in args:
            if isinstance(args[arg], list):
                args[arg] = json.dumps(args[arg])

        arg_strings = ["{}={}".format(arg, args[arg]) for arg in sorted(args.keys())]
        args_joined_string = ''.join(arg_strings)
        args_joined = _tobytes(args_joined_string)

        hash = hashlib.md5(args_joined)

        if secret:
            hash.update(_tobytes(secret))
        elif self.client.api_secret:
            hash.update(_tobytes(self.client.api_secret))
        return hash.hexdigest()

    def authenticate(self, url, params):
        """
        returns a request object ready to be issued to the Mixpanel API
        """
        params['api_key'] = self.client.api_key
        params['expire'] = int(time.time()) + self.DEFAULT_EXPIRATION

        # Creating signature
        if 'sig' in params:
            del params['sig']
        params['sig'] = self._hash_args(params, self.client.api_secret)

        request_url = '{base_url}?{encoded_params}'.format(
            base_url=url,
            encoded_params=_unicode_urlencode(params)
        )
        return url_request.Request(request_url)


class SecretAuth(object):
    """
    Secret-based authentication sends your api secret over https for verification
    by mixpanel.

    This method of authentication is the recommended authentication method.

    Please see https://mixpanel.com/help/reference/data-export-api#authentication
    for more details.
    """

    def __init__(self, client):
        self.client = client

    def authenticate(self, url, params):
        """
        returns a request object ready to be issued to the Mixpanel API
        """
        request_url = '{base_url}?{encoded_params}'.format(
            base_url=url,
            encoded_params=_unicode_urlencode(params)
        )
        request_headers = {
            'Authorization': 'Basic ' + _totext(base64.standard_b64encode(_tobytes("{}:".format(self.client.api_secret))))
        }
        return url_request.Request(request_url, headers=request_headers)
