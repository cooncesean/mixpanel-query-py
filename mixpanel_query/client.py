import datetime

from mixpanel_query import exceptions
from mixpanel_query.connection import Connection


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
    def annotations_list(self, start_date, end_date, response_format=FORMAT_JSON):
        """
        List the annotations for the given date range.

        Args:
            `start_date`: [str] The beginning of the date range to get annotations for in yyyy-mm-dd
                                format. This date is inclusive.
                          [sample]: "2014-04-01"
            `end_date`: [str] The end of the date range to get annotations for in yyyy-mm-dd format.
                              This date is inclusive.
                        [sample]: "2014-04-01"
            `response_format`: [string (optional)]: The data return format.
                               [sample]: "json" or "csv"

        Response format:
        {
            'annotations': [
                {'date': '2014-05-23 00:00:00', 'project_id': 23880, 'id': 148, 'description': 'Launched v2.0 of product'},
                {'date': '2014-05-29 00:00:00', 'project_id': 23880, 'id': 150, 'description': 'Streamlined registration process'}
            ],
            'error': false
        }
        """
        start_date_obj = self._validate_date(start_date)
        end_date_obj = self._validate_date(end_date)

        # Check the actual dates
        if start_date_obj > end_date_obj:
            raise exceptions.InvalidDateException('The `start_date` specified after the `end_date`; you will not receive any annotations.')

        return self.connection.request(
            'annotations',
            {
                'from_date': start_date,
                'to_date': end_date,
            },
            response_format=response_format
        )

    def annotation_create(self, date, description, response_format=FORMAT_JSON):
        """
        Create a new annotation at the specified time.

        Args:
            `date`: [str] The time in yyyy-mm-hh HH:MM:SS when you want to create the annotation at.
                    [sample]: "2014-04-01 02:12:44"
            `description`: [str] The annotation description.
                           [sample]: "Something happened on this date."
            `response_format`: [string (optional)]: The data return format.
                               [sample]: "json" or "csv"

        Response format:
            {
                'error': false
            }
        """
        date_obj = self._validate_date(date)
        return self.connection.request(
            'annotations/create',
            {
                'date': date_obj.strftime('%Y-%m-%d %H:%M:%S'),
                'description': description,
            },
            response_format=response_format
        )

    def annotation_update(self, annotation_id, date, description, response_format=FORMAT_JSON):
        """
        Update an existing annotation with a new description.

        Args:
            `annotation_id`: [int] The id of the annotation you wish to update.
                             [sample]: 1
            `date`: [str] The time in yyyy-mm-hh HH:MM:SS when you want to create the annotation at.
                    [sample]: "2014-04-01 02:12:44"
            `description`: [str] The annotation description.
                           [sample]: "Something happened on this date."
            `response_format`: [string (optional)]: The data return format.
                               [sample]: "json" or "csv"

        Response format:
            {
                'error': false
            }
        """
        date_obj = self._validate_date(date)
        return self.connection.request(
            'annotations/update',
            {
                'id': annotation_id,
                'date': date_obj.strftime('%Y-%m-%d %H:%M:%S'),
                'description': description,
            },
            response_format=response_format
        )

    def annotation_delete(self, annotation_id, response_format=FORMAT_JSON):
        """
        Delete an existing annotation.

        Args:
            `annotation_id`: [int] The id of the annotation you wish to delete.
                             [sample]: 1
            `response_format`: [string (optional)]: The data return format.
                               [sample]: "json" or "csv"

        Response format:
            {
                'error': false
            }
        """
        return self.connection.request(
            'annotations/delete',
            {
                'id': annotation_id,
            },
            response_format=response_format
        )

    # Event methods ###################
    def get_events_unique(self, event_names, unit, interval, response_format=FORMAT_JSON):
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
            },
            response_format=response_format
        )

    def get_events_general(self, event_names, unit, interval, response_format=FORMAT_JSON):
        """
        Get general event data for a set of event types over the last N days, weeks, or months.

        Args:
            - See `get_unique_events()` docstring.
        Reponse format:
            - See `get_unique_events()` docstring.
        """
        self._validate_unit(unit)
        self._validate_response_format(response_format)
        return self.connection.request(
            'events',
            {
                'event': event_names,
                'unit': unit,
                'interval': interval,
                'type': 'general'
            },
            response_format=response_format
        )

    def get_events_average(self, event_names, unit, interval, response_format=FORMAT_JSON):
        """
        Get averaged event data for a set of event types over the last N days, weeks, or months.

        Args:
            - See `get_unique_events()` docstring.
        Reponse format:
            - See `get_unique_events()` docstring.
        """
        self._validate_unit(unit)
        self._validate_response_format(response_format)
        return self.connection.request(
            'events',
            {
                'event': event_names,
                'unit': unit,
                'interval': interval,
                'type': 'average'
            },
            response_format=response_format
        )

    def get_events_top(self, event_name, limit=10, response_format=FORMAT_JSON):
        """
        Get the top property names for an event.

        Args:
            `event_name`: [str] Te event that you wish to get data for. Note: this is a single event name, not a list.
                          [sample]: "play song" or "log in"
            `limit`: [int (optional)] The maximum number of properties to return. Defaults to 10.
            `response_format`: [string (optional)]: The data return format.
                               [sample]: "json" or "csv"

        Response format:
        {
            'ad version': {
                'count': 295
            },
            'user type': {
                'count': 91
            }
        }
        """
        self._validate_response_format(response_format)
        return self.connection.request(
            'events/properties/top',
            {
                'event': event_name,
                'limit': limit,
            },
            response_format=response_format
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
            raise exceptions.InvalidUnitException('The `unit` specified is invalid. Must be: {0}'.format(self.VALID_UNITS))

    def _validate_response_format(self, response_format):
        " Utility method used to validate a `response_format` param. "
        if response_format not in self.VALID_RESPONSE_FORMATS:
            raise exceptions.InvalidFormatException('The `response_format` specified is invalid. Must be {0}.'.format(self.VALID_RESPONSE_FORMATS))

    def _validate_date(self, date):
        " Utility method used to validate a `response_format` param. "
        try:
            return datetime.datetime.strptime(date, '%Y-%m-%d')
        except ValueError:
            try:
                return datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                raise exceptions.InvalidDateException('The `date` specified is invalid. Must be in `YYYY-MM-DD` format.')
