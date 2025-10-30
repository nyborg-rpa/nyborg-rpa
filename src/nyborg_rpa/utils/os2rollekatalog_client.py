import os

import httpx
from dotenv import load_dotenv


class OS2rollekatalogClient(httpx.Client):

    def __init__(
        self,
        *,
        kommune: str,
        api_key: str | None = None,
        **kwargs,
    ):
        """
        Initialize the OS2rollekatalogClient.

        Args:
            api_key: API key for rollekatalog. Defaults to `OS2ROLLEKATALOG_API_KEY` environment variable if not provided.
            kommune: The `{kommune}.rollekatalog.dk/api` domain to connect to.
            kwargs: Extra arguments passed to httpx.Client.
        """

        if not api_key:
            load_dotenv(override=True, verbose=True)
            api_key = os.environ["OS2ROLLEKATALOG_API_KEY"]

        super().__init__(
            base_url=f"https://{kommune}.rollekatalog.dk/api",
            headers={"ApiKey": api_key},
            **kwargs,
        )

    def get_all_userroles(self) -> list[dict] | None:
        """
        Fetch all userroles.

        Returns:
            List of dict with userroles.
        """

        resp = self.get(url="read/userroles")
        resp.raise_for_status()

        userroles = resp.json()

        return userroles

    def get_userrole_details(self, role_name: str) -> dict | None:
        """
        Fetch specific userrole details.

        Args:
            userrole_name: The name of the userrole.

        Returns:
            Dict with details of userrole.

        """

        userroles = self.get_all_userroles()
        matches = [ur for ur in userroles if ur["name"] == role_name]
        if not matches:
            return None
        elif len(matches) > 1:
            raise ValueError(f"More than one match found by the name '{role_name}': {matches}")

        userrole = matches[0]

        params = {
            "indirectRoles": "true",
            "withDescription": "true",
        }

        resp = self.get(url=f"read/assigned/{userrole['id']}", params=params)
        resp.raise_for_status()

        userrole_details = resp.json()

        return userrole_details
