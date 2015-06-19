import datetime
import json

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

    DATA_TYPE_GENERAL = 'general'
    DATA_TYPE_AVERAGE = 'average'
    DATA_TYPE_UNIQUE = 'unique'
    VALID_DATA_TYPES = (DATA_TYPE_GENERAL, DATA_TYPE_AVERAGE, DATA_TYPE_UNIQUE)

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
    def get_events(self, event_names, unit, interval, data_type=DATA_TYPE_UNIQUE, response_format=FORMAT_JSON):
        """
        Get unique, total, or average data for a set of events over the last N days,
        weeks, or months.

        Args:
            - See `get_unique_events()` docstring.
        Reponse format:
            - See `get_unique_events()` docstring.
        """
        self._validate_unit(unit)
        self._validate_response_format(response_format)
        self._validate_data_type(data_type)
        return self.connection.request(
            'events',
            {
                'event': event_names,
                'unit': unit,
                'interval': interval,
                'type': data_type,
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

    def get_event_top_names(self, data_type='general', limit=255, response_format=FORMAT_JSON):
        """
        Get a list of the most common events over the last 31 days; ordered by volume, descending

        Args:
            `event_type`: [str] The analysis type you would like to get data for.
                          [sample]: Valid values: 'general', 'unique', or 'average'
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
            'events/names',
            {
                'type': data_type,
                'limit': limit,
            },
            response_format=response_format
        )

    # Event properties methods ########
    def get_event_properties(
            self, event_name, property_name, filter_values, unit,
            interval, data_type='general', limit=255,
            response_format=FORMAT_JSON):
        """
        Get unique, total, or average data for of a single event and property
        over the last N days, weeks, or months.

        Response format:
        {
            'data': {
                'series': ['2010-05-29', '2010-05-30', '2010-05-31'],
                'values': {
                    'splash features': {
                        '2010-05-29': 6,
                        '2010-05-30': 4,
                        '2010-05-31': 5,

                    }
                }
            },
            'legend_size': 2
        }
        """
        self._validate_response_format(response_format)
        return self.connection.request(
            'events/properties',
            {
                'event': event_name,
                'name': property_name,
                'values': filter_values,
                'unit': unit,
                'interval': interval,
                'type': data_type,
                'limit': limit,
            },
            response_format=response_format
        )

    def get_event_properties_top(self, event_name, limit=10, response_format=FORMAT_JSON):
        """
        Get the top property names for an event.

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

    def get_event_properties_values(self, event_name, property_name, limit=255, bucket_id=None, response_format=FORMAT_JSON):
        """
        Get the top values for a property ordered by volume, descending.

        Response format:
            ['male', 'female', 'unknown']
        """
        self._validate_response_format(response_format)

        params = {
            'event': event_name,
            'name': property_name,
            'limit': limit,
        }
        if bucket_id:
            params.update({'bucket': bucket_id})

        return self.connection.request(
            'events/properties/values',
            params,
            response_format=response_format
        )

    # Funnel methods ##################
    def get_funnel_list(self, response_format=FORMAT_JSON):
        """
        Get the names and funnel_ids of your funnels. This method takes no parameters.

        Response format:
        [
            {"funnel_id": 7509, "name": "Signup funnel"},
            {"funnel_id": 9070, "name": "Funnel tutorial"}
        ]
        """
        self._validate_response_format(response_format)
        return self.connection.request(
            'funnels/list',
            {},
            response_format=response_format
        )

    def get_funnel_detail(
            self, funnel_id, start_date=None, end_date=None,
            length=14, interval=1, unit=UNIT_DAY, on=None, where=None, limit=None,
            response_format=FORMAT_JSON):
        """
        Get data for a specified funnel.

        Response format:
        {
            'Signup flow': {
                'data': {
                    '2010-05-24': {
                        'analysis': {
                            'completion': 0.064679359580052493,
                            'starting_amount': 762,
                            'steps': 3,
                            'worst': 2
                        },
                        'steps': [
                            {
                                'count': 762,
                                'goal': 'pages',
                                'overall_conv_ratio': 1.0,
                                'step_conv_ratio': 1.0
                            },
                            {
                                'count': 69,
                                'goal': 'View signup',
                                'overall_conv_ratio': 0.09055118110236221,
                                'step_conv_ratio': 0.09055118110236221
                            },
                            {
                                'count': 10,
                                'goal': 'View docs',
                                'overall_conv_ratio': 0.064679359580052493,
                                'step_conv_ratio': 0.7142857142857143
                            }
                        ]
                    },
                    '2010-05-31': {
                        'analysis': {
                            'completion': 0.12362030905077263,
                            'starting_amount': 906,
                            'steps': 2,
                            'worst': 2
                        },
                        'steps': [
                            {
                                'count': 906,
                                'goal': 'homepage',
                                'overall_conv_ratio': 1.0,
                                'step_conv_ratio': 1.0
                            },
                            {
                                'count': 112,
                                'goal': 'View signup',
                                'overall_conv_ratio': 0.12362030905077263,
                                'step_conv_ratio': 0.12362030905077263
                            }
                        ]
                    }
                },
            'meta': {'dates': ['2010-05-24', '2010-05-31']}
            }
        }
        """
        self._validate_response_format(response_format)
        start_date_obj = self._validate_date(start_date)
        end_date_obj = self._validate_date(end_date)
        # self._validate_expression(on, where)

        # Check the actual dates
        if start_date_obj > end_date_obj:
            raise exceptions.InvalidDateException('The `start_date` specified after the `end_date`; you will not receive any annotations.')

        return self.connection.request(
            'funnels',
            {
                'funnel_id': funnel_id,
                'from_date': start_date,
                'to_date': end_date,
                'length': length,
                'interval': interval,
                'unit': unit,
                'on': on,
                'where': where,
            },
            response_format=response_format
        )

    # Segmentation methods ############
    def get_segmentation(
            self, event_name, start_date, end_date,
            unit=UNIT_DAY, on=None, where=None, limit=None,
            data_type=DATA_TYPE_UNIQUE, response_format=FORMAT_JSON):
        """
        Get data for an event, segmented and filtered by properties.

        # Example 1
        Suppose Kevin Wood has a website named guidebook.com. He has an event named
        signed up, sent whenever a user signs up to example.com. It has a string
        property named `mp_country_code` that stores the country code of the
        user signing up.

        > user_client.get_segmentation('signed up', '2011-08-06', ' 2011-08-16')
        {
            'data': {
                'series': ['2011-08-08', '2011-08-09', '2011-08-06', '2011-08-07'],
                'values': {
                    'signed up': {
                        '2011-08-06': 147,
                        '2011-08-07': 146,
                        '2011-08-08': 776,
                        '2011-08-09': 1376
                    }
                }
            },
            'legend_size': 1
        }

        # Example 2
        Suppose Kevin is impressed with the number of signups on 2011-08-09, and now
        wants to know the top five countries his signups came from on that day. He
        can make the following query:

        > user_client.get_segmentation('signed up', '2011-08-09', ' 2011-08-09', limit=5, on='properties["mp_country_code"]')
        {
            'data': {
                'series': ['2011-08-09'],
                'values': {
                    'CA': {'2011-08-09': 277},
                    'FR': {'2011-08-09': 8},
                    'GB': {'2011-08-09': 19},
                    'IN': {'2011-08-09': 19},
                    'US': {'2011-08-09': 1036}
                }
            },
            'legend_size': 5
        }

        # Example 3
        Kevin now wants to zero in on the US and Canada. He is tracking a property
        named mp_keyword, which tells him the search keyword that users used to
        get to example.com. Now he wants to determine how many signups from the US
        and Canada came about as a result of a search that contained the word 'guidebook.'
        He can do that with the following query:

        > user_client.get_segmentation(
            'signed up',
            '2011-08-09',
            '2011-08-09',
            on='properties["mp_country_code"]',
            where='"guidebook" in properties["mp_keyword"] and ("CA" == properties["mp_country_code"] or "US" == properties["mp_country_code"])'
        )
        {
            'data': {
                'series': ['2011-08-09'],
                'values': {
                    'CA': {'2011-08-09': 31},
                    'US': {'2011-08-09': 312}
                }
            },
            'legend_size': 2
        }
        """
        self._validate_response_format(response_format)
        #self._validate_expression(on, where)
        start_date_obj = self._validate_date(start_date)
        end_date_obj = self._validate_date(end_date)

        # Check the actual dates
        if start_date_obj > end_date_obj:
            raise exceptions.InvalidDateException('The `start_date` specified after the `end_date`; you will not receive any annotations.')

        return self.connection.request(
            'segmentation',
            {
                'event': event_name,
                'from_date': start_date,
                'to_date': end_date,
                'unit': unit,
                'on': on,
                'where': where,
                'limit': limit,
                'type': data_type,
            },
            response_format=response_format
        )

    def get_segmentation_numeric(
            self, event_name, start_date, end_date, on,
            unit=UNIT_DAY, where=None,
            buckets=None, data_type=DATA_TYPE_UNIQUE,
            response_format=FORMAT_JSON):
        """
        Get data for an event, segmented and filtered by properties, with values placed into numeric buckets.

        # Example 1
        Kevin also has an event named page loaded. It has a property named time
        that represents the time in milliseconds it took to load the page. Kevin
        wants to see the distribution of page load times that are greater than
        2000 milliseconds. But suppose Harry accidentally was sending the time
        property to Mixpanel as a string, so its the incorrect type. He can make
        the following query involving an explicit typecast to number to get the
        results he wants:

        > user_client.get_segmentation_numeric(
            'page loaded',
            '2011-08-06',
            '2011-08-16',
            on='number(properties["time"])',
            where='number(properties["time"]) >= 2000',
            buckets=5
        )
        {
            'data': {
                'series': ['2011-08-08', '2011-08-09', '2011-08-06', '2011-08-07'],
                'values': {
                    '2,000 - 2,100': {
                        '2011-08-06': 1,
                        '2011-08-07': 5,
                        '2011-08-08': 4,
                        '2011-08-09': 15
                    },
                    '2,100 - 2,200': {
                        '2011-08-07': 2,
                        '2011-08-08': 7,
                        '2011-08-09': 15
                    },
                    '2,200 - 2,300': {
                        '2011-08-06': 1,
                        '2011-08-08': 6,
                        '2011-08-09': 5
                    },
                    '2,300 - 2,400': {
                        '2011-08-06': 4,
                        '2011-08-08': 1,
                        '2011-08-09': 12
                    },
                    '2,400 - 2,500': {
                        '2011-08-08': 2,
                        '2011-08-09': 5
                    }
                }
            },
            'legend_size': 5
        }
        """
        self._validate_response_format(response_format)
        #self._validate_expression(on, where)
        start_date_obj = self._validate_date(start_date)
        end_date_obj = self._validate_date(end_date)

        # Check the actual dates
        if start_date_obj > end_date_obj:
            raise exceptions.InvalidDateException('The `start_date` specified after the `end_date`; you will not receive any annotations.')

        return self.connection.request(
            'segmentation/numeric',
            {
                'event': event_name,
                'from_date': start_date,
                'to_date': end_date,
                'unit': unit,
                'on': on,
                'where': where,
                'buckets': buckets,
                'type': data_type,
            },
            response_format=response_format
        )

    def get_segmentation_sum(
            self, event_name, start_date, end_date, on,
            unit=UNIT_DAY, where=None,
            response_format=FORMAT_JSON):
        """
        Sums an expression for events per unit time.

        # Example 1
        Kevin also sells things from example.com. He has an event named item sold
        that tracks each item that gets sold from his website. It has a number property
        named price that records the value of the item being sold. He has another number
        property named overhead that represents the overhead cost of the item. Kevin
        can find out how much profit he is making each day with the following query:

        > user_client.get_segmentation_sum(
            'item sold',
            '2011-08-06',
            '2011-08-16',
            on='properties["price"] - properties["overhead"]',
        )
        {   'results': {
                '2011-08-06': 376.0,
                '2011-08-07': 634.0,
                '2011-08-08': 474.0,
                '2011-08-09': 483.0
        },
            'status': 'ok'
        }
        """
        self._validate_response_format(response_format)
        #self._validate_expression(on, where)
        start_date_obj = self._validate_date(start_date)
        end_date_obj = self._validate_date(end_date)

        # Check the actual dates
        if start_date_obj > end_date_obj:
            raise exceptions.InvalidDateException('The `start_date` specified after the `end_date`; you will not receive any annotations.')

        return self.connection.request(
            'segmentation/sum',
            {
                'event': event_name,
                'from_date': start_date,
                'to_date': end_date,
                'unit': unit,
                'on': on,
                'where': where
            },
            response_format=response_format
        )

    def get_segmentation_average(
            self, event_name, start_date, end_date,
            unit=UNIT_DAY, on=None, where=None,
            response_format=FORMAT_JSON):
        """
        Averages an expression for events per unit time.

        # Example 1
        Instead of finding out the total profit he is making per day by selling things
        from his website, Kevin can also find out the average price of an item being
        sold with the following query:

        > user_client.get_segmentation_average(
            'item sold',
            '2011-08-06',
            '2011-08-16',
            on='properties["price"] - properties["overhead"]',
        )
        {
            'results': {
                '2011-08-06': 8.64705882352939,
                '2011-08-07': 4.640625,
                '2011-08-08': 3.6230899830221,
                '2011-08-09': 7.3353658536585
            },
            'status': 'ok'
        }
        """
        self._validate_response_format(response_format)
        #self._validate_expression(on, where)
        start_date_obj = self._validate_date(start_date)
        end_date_obj = self._validate_date(end_date)

        # Check the actual dates
        if start_date_obj > end_date_obj:
            raise exceptions.InvalidDateException('The `start_date` specified after the `end_date`; you will not receive any annotations.')

        return self.connection.request(
            'segmentation/average',
            {
                'event': event_name,
                'from_date': start_date,
                'to_date': end_date,
                'unit': unit,
                'on': on,
                'where': where,
                'unit': unit,
            },
            response_format=response_format
        )

    def get_segmentation_multiseg(
            self, event_name, start_date, end_date,
            unit=UNIT_DAY, inner=None, outer=None,
            data_type=DATA_TYPE_GENERAL, where=None,
            limit=None, response_format=FORMAT_JSON):
        """
        WARNING THIS IS AN UNDOCUMENTED API ENDPOINT
        USE AT YOUR OWN RISK, MIXPANEL MAY CHANGE THIS

        This allows a user to segment on two properties rather
        just one. Example:

        > user_client.get_segmentation_multiseg(
            'item sold',
            '2011-08-06',
            '2011-08-16',
            inner='properties["$city"]',
            outer='properties["$region"]',
        )
        {   
        u'data': {   
            u'series': [u'2015-06-01', u'2015-06-02', u'2015-06-03'],
            u'values': {   
                u'North Carolina': {   
                    u'Charlotte': {   
                        u'2015-06-01': 300,
                        u'2015-06-02': 111,
                        u'2015-06-03': 171
                    },
                    u'Austin': {   
                        u'2015-06-01': 0,
                        u'2015-06-02': 0,
                        u'2015-06-03': 0
                    }
                },
                u'Texas': {   
                    u'Charlotte': {   
                        u'2015-06-01': 0,
                        u'2015-06-02': 0,
                        u'2015-06-03': 0},
                    u'Austin': {   
                        u'2015-06-01': 3181,
                        u'2015-06-02': 3219,
                        u'2015-06-03': 3484
                    }
                }
            }
        },
        u'legend_size': 4
        }


        Note the way this works. Limit defines the range for both the inner
        outer properties. The top <limit> outer segments are matched against
        the top <limit> inner segments. In the case of region ond city
        this results in many nested segments with zero results. At this time
        you do not get the top inner segments per outer segment.
        """
        self._validate_response_format(response_format)
        #self._validate_expression(inner, outer, where)
        start_date_obj = self._validate_date(start_date)
        end_date_obj = self._validate_date(end_date)

        # Check the actual dates
        if start_date_obj > end_date_obj:
            raise exceptions.InvalidDateException('The `start_date` specified after the `end_date`; you will not receive any annotations.')

        return self.connection.request(
            'segmentation/multiseg',
            {
                'event': event_name,
                'from_date': start_date,
                'to_date': end_date,
                'unit': unit,
                'inner': inner,
                'outer': outer,
                'where': where,
                'type': data_type,
                'limit': limit,
            },
            response_format=response_format
        )

    # Retention methods ###############

    # People methods ##################
    def get_engage(self, where=None, session_id=None, page=None, response_format=FORMAT_JSON):
        """
        Query People Data.

        Reponse format:
            {'page': 0,
             'page_size': 1000,
             'results': [{'$distinct_id': 4,
                          '$properties': {'$created': '2008-12-12T11:20:47',
                                          '$email': 'example@mixpanel.com',
                                          '$first_name': 'Example',
                                          '$last_name': 'Name',
                                          '$last_seen': '2008-06-09T23:08:40',}}],
             'session_id': '1234567890-EXAMPL',
             'status': 'ok',
             'total': 1}
        """
        return self.connection.request(
            'engage',
            {
                'where': where,
                'session_id': session_id,
                'page': page,
            },
            response_format=response_format
        )

    # Export methods ##################
    def get_export(self, start_date, end_date, event=None, where=None, bucket_id=None, response_format=FORMAT_JSON):
        """
        Get a "raw dump" of tracked events over a time period.

        Yields events as they are returned (matching the format shown below).

        Args:
            `start_date`: [str] The date in yyyy-mm-dd format from which to begin querying for the
                                event from. This date is inclusive.
                          [sample]: "2014-04-01"
            `end_date`: [str] The date in yyyy-mm-dd format from which to stop querying for the
                              event from. This date is inclusive.
                        [sample]: "2014-04-01"
            `event`: [array (optional)]: The event or events that you wish to get data for.
                               [sample]: ["play song", "log in", "add playlist"]
            `where`: [str] An expression to filter events by.
            `bucket_id`: [str] The specific data bucket you would like to query.

        Event format:
            {"event":"Viewed report","properties":{"distinct_id":"foo","time":1329263748,"origin":"invite",
            "origin_referrer":"http://mixpanel.com/projects/","$initial_referring_domain":"mixpanel.com",
            "$referrer":"https://mixpanel.com/report/3/stream/","$initial_referrer":"http://mixpanel.com/",
            "$referring_domain":"mixpanel.com","$os":"Linux","origin_domain":"mixpanel.com","tab":"stream",
            "$browser":"Chrome","Project ID":"3","mp_country_code":"US"}}
        """
        start_date_obj = self._validate_date(start_date)
        end_date_obj = self._validate_date(end_date)

        # Check the actual dates
        if start_date_obj > end_date_obj:
            raise exceptions.InvalidDateException('The `start_date` specified after the `end_date`; you will not receive any events.')

        if isinstance(event, str):
            event = [event]

        response = self.connection.raw_request(
            Connection.DATA_ENDPOINT,
            'export',
            {
                'from_date': start_date,
                'to_date': end_date,
                'event': event,
                'where': where,
                'bucket': bucket_id,
            },
            response_format
        )
        for line in response:
            yield json.loads(line)

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

    def _validate_data_type(self, data_type):
        " Utility method used to validate a `data_type` param. "
        if data_type not in self.VALID_DATA_TYPES:
            raise exceptions.InvalidDataType('The `data_type` specified is invalid.  Must be {0}'.format(self.VALID_DATA_TYPES))

    def _validate_expression(self, on, where):
        " Validate the expression by these rules: https://mixpanel.com/docs/api-documentation/data-export-api#segmentation-expressions ."
        raise NotImplementedError('This is not yet complete.')
