class MixpanelQueryException(Exception):
    pass

class InvalidUnitException(MixpanelQueryException):
    " An invalid time unit was passed. "
    pass

class InvalidFormatException(MixpanelQueryException):
    " An invalid response format was passed. "
    pass

class InvalidAPIKeyException(MixpanelQueryException):
    " An invalid API key + secret were passed. Check your account page for the correct key. "
    pass

class ExpiredRequestException(MixpanelQueryException):
    " The request is past its expiration date (default 10 minutes). "
    pass

class InvalidDateException(MixpanelQueryException):
    " The date range you have specified is not 30 days or less. "
    pass

class InvalidDataType(MixpanelQueryException):
    " The data type you have specified is invalid. "
    pass
