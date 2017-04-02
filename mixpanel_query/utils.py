import json

import six

from six.moves.urllib.parse import urlencode


def _totext(val):
    """
    Py2 and Py3 compatible function that coerces
    any non-Unicode string types into the official "text type" (unicode in Py2, str in Py3).
    For objects that are not binary (str/bytes) or text (unicode/str),
    return value is unchanged object.
    """
    if isinstance(val, six.text_type):
        return val
    elif isinstance(val, six.binary_type):
        return val.decode('utf-8')
    else:
        return val

def _tobytes(val):
    """
    Py2 and Py3 compatible function that coerces
    any non-binary string types into the official "binary type" (str in Py2, bytes in Py3).
    Values that are not a text or binary type are converted to text (unicode)
    first, then encoded via UTF-8 into binary type.
    """
    if isinstance(val, six.binary_type):
        return val
    elif isinstance(val, six.text_type):
        return val.encode('utf-8')
    else:
        return six.text_type(val).encode('utf-8')

def _unicode_urlencode(params):
    """
    Convert lists to JSON encoded strings, and correctly handle any
    unicode URL parameters.
    """
    if isinstance(params, dict):
        params = list(six.iteritems(params))
    for i, param in enumerate(params):
        if isinstance(param[1], list):
            params[i] = (param[0], json.dumps(param[1]),)

    return urlencode(
        [(_tobytes(k), _tobytes(v)) for k, v in params]
    )
