import unittest

from nose.tools import nottest

from mixpanel_query.paginator import ConcurrentPaginator


RESULTS = [i for i in range(0, 100)]
def get_function():
    " 'mocks' the `Client.engage()` method and returns a typical MixPanel response. "
    return {
        'page': 0,
        'page_size': 10,
        'results': RESULTS,
        'session_id': '1234567890-EXAMPL',
        'status': 'ok',
        'total': 1
    }

class TestConcurrentPaginator(unittest.TestCase):
    " Unit tests for the `paginator.ConcurrentPaginator` class. "

    def test_fetchall(self):
        """
        Assert that fetchall() returns the expecte results for the specified
        page
        """
        # Instantiate the paginator and fetchall results
        paginator = ConcurrentPaginator(get_function)
        self.assertEquals(paginator.fetch_all(), RESULTS)

    @nottest
    def test_fetch_page(self):
        raise NotImplementedError('This test functionality does not exist currently.')
