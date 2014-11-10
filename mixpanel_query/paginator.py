import math
import itertools
from multiprocessing.pool import ThreadPool

class ConcurrentPaginator(object):
    """
    Concurrently fetches all pages in a paginated collection.

    Currently, only the people API (`/api/2.0/engage`) supports pagination.
    This class is designed to support the people API's implementation of
    pagination.
    """

    def __init__(self, get_func, concurrency=20):
        """
        Initialize with a function that fetches a page of results.
        `concurrency` controls the number of threads used to fetch pages.

        Example:
            client = MixpanelQueryClient(...)
            ConcurrentPaginator(client.get_engage, concurrency=10)
        """
        self.get_func = get_func
        self.concurrency = concurrency

    def fetch_all(self, params=None):
        """
        Fetch all results from all pages, and return as a list.

        If params need to be sent with each request (in addition to the
        pagination) params, they may be passed in via the `params` kwarg.
        """
        params = params and params.copy() or {}

        first_page = self.get_func(**params)
        results = first_page['results']
        params['session_id'] = first_page['session_id']

        start, end = self._remaining_page_range(first_page)
        fetcher = self._results_fetcher(params)
        return results + self._concurrent_flatmap(fetcher, range(start, end))

    def _results_fetcher(self, params):
        def _fetcher_func(page):
            req_params = dict(params.items() + [('page', page)])
            return self.get_func(**req_params)['results']
        return _fetcher_func

    def _concurrent_flatmap(self, func, iterable):
        pool = ThreadPool(processes=self.concurrency)
        return list(itertools.chain(*pool.map(func, iterable)))

    def _remaining_page_range(self, response):
        num_pages = math.ceil(response['total'] / float(response['page_size']))
        return (response['page'] + 1, int(num_pages))

