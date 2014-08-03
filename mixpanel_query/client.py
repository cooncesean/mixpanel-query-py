from mixpanel_query.connection import Connection
from mixpanel_query.exceptions import InvalidUnitException, InvalidFormatException


class MixpanelQueryClient(object):
    """
    Connects to the `Mixpanel Data Export API`
    and provides an interface to query data based on the project
    specified with your api credentials.

    Full API Docs: https://mixpanel.com/docs/api-documentation/data-export-api
    """
    ENDPOINT = 'http://mixpanel.com/api'
    VERSION = '2.0'

    UNIT_MINUTE = 'minute'
    UNIT_HOUR = 'hour'
    UNIT_DAY = 'day'
    UNIT_WEEK = 'week'
    UNIT_MONTH = 'month'
    VALID_UNITS = (UNIT_MINUTE, UNIT_HOUR, UNIT_DAY, UNIT_WEEK, UNIT_MONTH)

    FORMAT_JSON = 'json'
    FORMAT_CSV = 'csv'
    VALID_RESPONSE_FORMATS = (FORMAT_JSON, FORMAT_CSV)

    def __init__(self, api_key, api_secret):
        self.api_key = api_key
        self.api_secret = api_secret
        self.connection = Connection(self)

    # Annotation methods ##############

    # Event methods ###################
    def get_unique_events(self, event_names, unit, interval, response_format=FORMAT_JSON):
        """
        Get unique event data for a set of event types over the last N days, weeks, or months.

        Args:
            `event_names`: [list] The event or events that you wish to get data for.
                           [sample]: ["play song", "log in", "add playlist"]
            `unit`: [str] Determines the level of granularity of the data you get back.
                    [sample]: "day" or "month" or "week"
            `interval`: [int] The number of "units" to return data for. `1` will return data for the
                              current unit (minute, hour, day, week or month). `2` will return the
                              current and previous units, and so on.
            `response_format`: [string (optional)]: The data return format.
                               [sample]: "json" or "csv"

        Response format:
        {
            u'data': {
                u'series': [u'2014-07-11', u'2014-07-12', u'2014-07-13'],
                u'values': {
                    u'Guide Download': {
                        u'2014-07-11': 80,
                        u'2014-07-12': 100,
                        u'2014-07-13': 123,  # Date + unique event counts
                    }
                }
            },
             u'legend_size': 1
        }
        """
        self._validate_unit(unit)
        self._validate_response_format(response_format)
        return self.connection.request(
            'events',
            {
                'event': event_names,
                'unit': unit,
                'interval': interval,
                'type': 'unique'
            }
        )

    # Event properties methods ########
    # Funnel methods ##################
    # Segmentation methods ############
    # Retention methods ###############
    # People methods ##################

    # Util methods ####################
    def _validate_unit(self, unit):
        " Utility method used to validate a `unit` param. "
        if unit not in self.VALID_UNITS:
            raise InvalidUnitException('The `unit` specified is invalid. Must be: {0}'.format(self.VALID_UNITS))

    def _validate_response_format(self, response_format):
        " Utility method used to validate a `response_format` param. "
        if response_format not in self.VALID_RESPONSE_FORMATS:
            raise InvalidFormatException('The `response_format` specified is invalid. Must be {0}.'.format(self.VALID_RESPONSE_FORMATS))
