"""HTTP-specific exceptions are defined here."""

from pmv2.urban_client.exceptions import APIError


class InvalidStatusCode(APIError):
    """Got unexpected status code from API request."""
