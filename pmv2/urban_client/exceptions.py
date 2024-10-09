"""Urban API client base exceptions are defined here."""


class APIError(RuntimeError):
    """Generic Urban API error."""


class APIConnectionError(APIError):
    """Could not connect to the API."""


class APITimeoutError(APIError, TimeoutError):
    """Timed out while awaiting response from UrbanAPI."""
