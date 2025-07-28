from typing import Optional, Dict, Any


class AirQualityException(Exception):
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.details = details or {}


class DataSourceException(AirQualityException):
    pass


class DataSourceError(DataSourceException):
    pass


class APIError(DataSourceException):
    pass


class RateLimitException(DataSourceException):
    def __init__(self, message: str, retry_after: Optional[int] = None, **kwargs):
        super().__init__(message, kwargs)
        self.retry_after = retry_after


class AuthenticationException(DataSourceException):
    pass


class DataValidationException(AirQualityException):
    pass


class StorageException(AirQualityException):
    pass


class CheckpointException(StorageException):
    pass


class ConfigurationException(AirQualityException):
    pass


class NetworkException(AirQualityException):
    def __init__(self, message: str, url: Optional[str] = None, status_code: Optional[int] = None, **kwargs):
        details = kwargs.copy()  # Avoid modifying kwargs directly
        if url and 'url' not in details:
            details['url'] = url
        if status_code and 'status_code' not in details:
            details['status_code'] = status_code
        super().__init__(message, details)
        self.url = details.get('url', url)
        self.status_code = details.get('status_code', status_code)