"""Urban API client base exceptions are defined here."""

class APIError(RuntimeError):
    """Generic Urban API error."""

class APIConnectionError(APIError):
    """Could not connect to the API."""
