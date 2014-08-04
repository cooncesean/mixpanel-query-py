# mixpanel-query-py

The Python interface to fetch data from Mixpanel via [Mixpanel's Data Query API](https://mixpanel.com/docs/api-documentation/data-export-api). Note, this differs from the official [Python binding](https://github.com/mixpanel/mixpanel-python) which only provides an interface to send data to Mixpanel.

# Installation

To install mixpanel-query-py, simply:

```
$ pip install mixpanel-query-py
```

or alternatively (you really should be using pip though):

```
$ easy_install mixpanel-query-py
```

or from source:

```
$ git clone git@github.com:cooncesean/mixpanel-query-py.git
$ cd mixpanel-query-py
$ python setup.py install
```

# Usage

You will need a [Mixpanel account](https://mixpanel.com/register/) and your `API_KEY` + `API_SECRET` to access your project's data via their API; which can be found in "Account" > "Projects".

```python
from mixpanel_query.client import MixpanelQueryClient
from your_project.conf import MIXPANEL_API_KEY, MIXPANEL_API_SECRET

# Instantiate the client
query_client = MixpanelQueryClient(MIXPANEL_API_KEY, MIXPANEL_API_SECRET)

# Query your project's data
data = query_client.get_events_unique(['Some Event Name'], 'hour', 24)
print data
{
    'data': {
        'series': ['2010-05-29', '2010-05-30', '2010-05-31'],
        'values': {
            'account-page': {'2010-05-30': 1},
            'splash features': {
                '2010-05-29': 6,
                '2010-05-30': 4,
                '2010-05-31': 5,  # Date + unique event counts
            }
        }
    },
    'legend_size': 2
}
```

View the [api reference](#api-reference) for details on accessing different endpoints.

# API Reference

Mixpanels' full [API reference is documented here](https://mixpanel.com/docs/api-documentation/data-export-api).

