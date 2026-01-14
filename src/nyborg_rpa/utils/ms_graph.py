from typing import Any

from authlib.integrations.httpx_client import OAuth2Client


class MSGraphClient(OAuth2Client):
    """Microsoft Graph API client with automatic token management and pagination support."""

    def __init__(self, tenant_id: str, client_id: str, client_secret: str):
        """
        Initialize the MS Graph client with OAuth2 credentials.

        Args:
            tenant_id: Azure AD tenant ID
            client_id: Application (client) ID
            client_secret: Client secret value
        """

        super().__init__(
            client_id=client_id,
            client_secret=client_secret,
            scope="https://graph.microsoft.com/.default",
            token_endpoint=f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
            timeout=30.0,
            base_url="https://graph.microsoft.com/v1.0/",
        )

        self.fetch_token()

    def get_paged(self, url: str, params: dict | None = None) -> list[dict[str, Any]]:
        """
        Get all results from a paginated Graph API endpoint.

        Args:
            url: API endpoint path (e.g., "users") or full URL
            params: Optional query parameters

        Returns:
            List of all items across all pages
        """

        results = []
        while url:

            resp = self.get(url, params=params)
            resp.raise_for_status()

            data = resp.json()
            results.extend(data.get("value", []))

            url = data.get("@odata.nextLink")
            params = None  # nextLink already has all params

        return results
