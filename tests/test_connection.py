import unittest


class TestConnection(unittest.TestCase):
    " Unit tests for the `connection.Connection` class. "

    def test_request(self):
        """
        Assert that request() calls raw_request with the appropriate args
        and parses the returned json value.
        """

    def test_raw_request(self):
        """
        Assert that raw_request builds the appropriate params and signature
        to pass to urllib.urlopen.
        """
