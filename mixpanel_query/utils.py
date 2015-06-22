import six

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
