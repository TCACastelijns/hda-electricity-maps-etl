import logging
from typing import Any

import requests

logger = logging.getLogger(__name__)


class ElectricityMapsClient:
    """Minimal MVP client for Electricity Maps v4 API.

    This keeps a single-request `_get` helper and simple `fetch_mix`/`fetch_flows`
    that accept an explicit `zone` argument.
    """

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://api.electricitymaps.com/v4",
        timeout_seconds: int = 30,
        session: requests.Session | None = None,
    ) -> None:
        """Create a client for the Electricity Maps v4 API.

        Args:
            api_key: API token used in the ``auth-token`` header.
            base_url: Base URL for the Electricity Maps API.
            timeout_seconds: Request timeout in seconds.
            session: Optional custom ``requests.Session`` to reuse.

        """
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.session = session or requests.Session()
        # Use the legacy `auth-token` header by default (preferred by the API).
        # Keep `api_key` available for debugging/logging.
        self.api_key = api_key
        self.session.headers.update({"auth-token": api_key})

    def _get(self, path: str, params: dict[str, Any]) -> tuple[dict[str, Any], str]:
        """Issue a GET request against the API and return decoded JSON plus the URL.

        Args:
            path: API path relative to the configured base URL.
            params: Query-string parameters for the request.

        Returns:
            A tuple of the decoded JSON payload and the final request URL.

        Raises:
            requests.HTTPError: If the API responds with a non-200 status code.

        """
        url = f"{self.base_url}/{path.lstrip('/')}"
        resp = self.session.get(url, params=params, timeout=self.timeout_seconds)
        response_code = 200
        if resp.status_code != response_code:
            # Mask auth values for logs using session headers
            hdrs = dict(self.session.headers)
            max_chars = 8
            masked = {
                k: (
                    v[:4] + "..." + v[-4:]
                    if isinstance(v, str) and len(v) > max_chars
                    else "***"
                )
                for k, v in hdrs.items()
            }
            logger.error(
                f"ElectricityMaps GET {resp.url} returned {resp.status_code}; "
                f"headers={masked}; body={resp.text}"
            )
            raise requests.HTTPError(
                f"Electricity Maps request failed: {resp.status_code}: {resp.text}",
                response=resp,
            )

        try:
            payload = resp.json()
        except ValueError:
            payload = {"raw": resp.text}

        return payload, resp.url

    def fetch_mix(self, zone: str) -> tuple[dict[str, Any], str]:
        """Fetch hourly generation mix data for a zone.

        Args:
            zone: Electricity Maps zone code, such as "FR".

        Returns:
            A tuple containing the parsed JSON payload and the request URL.

        """
        return self._get(
            "/electricity-mix/history",
            {
                "zone": zone,
                "temporalGranularity": "hourly",
            },
        )

    def fetch_flows(self, zone: str) -> tuple[dict[str, Any], str]:
        """Fetch hourly cross-border flow data for a zone.

        Args:
            zone: Electricity Maps zone code, such as "FR".

        Returns:
            A tuple containing the parsed JSON payload and the request URL.

        """
        return self._get(
            "/electricity-flows/history",
            {
                "zone": zone,
                "temporalGranularity": "hourly",
            },
        )

    def fetch_zone_metadata(self) -> tuple[dict[str, Any], str]:
        """Fetch metadata for all supported zones.

        Returns:
            A tuple containing the parsed metadata payload and the request URL.

        """
        return self._get(
            "/zones",
            {},
        )
