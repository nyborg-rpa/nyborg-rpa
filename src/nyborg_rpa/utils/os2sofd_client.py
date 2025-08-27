import os

import httpx
from dotenv import load_dotenv


class OS2sofdClient(httpx.Client):

    def __init__(
        self,
        *,
        kommune: str,
        api_key: str | None = None,
        **kwargs,
    ):
        """
        Initialize the SOFDClient with API key and base URL.

        Args:
            api_key: API key for SOFD. If not given, pulled from OS2SOFD_API_KEY in .env.
            kommune: The `{kommune}.sofd.io` domain to connect to.
            kwargs: Extra arguments passed to httpx.Client.
        """

        if not api_key:
            load_dotenv(override=True, verbose=True)
            api_key = os.environ["OS2SOFD_API_KEY"]

        super().__init__(
            base_url=f"https://{kommune}.sofd.io",
            headers={"ApiKey": api_key},
            **kwargs,
        )

    def get_user_by_cpr(self, cpr: str) -> dict | None:
        """
        Fetch user information based on CPR number.

        Args:
            cpr: The CPR number of the user.

        Returns:
            dict: User information if found, otherwise None.
        """
        params = {
            "$filter": f"Cpr eq '{cpr}'",
            "$expand": "Affiliations,Users,Photo,Phones,Children,AuthorizationCodes,Substitutes,DisabledUsers",
        }

        resp = self.get(url="odata/Persons", params=params)
        resp.raise_for_status()

        data = resp.json()
        user = next(iter(data.get("value", [])), None)

        return user


if __name__ == "__main__":
    # Example usage
    client = OS2sofdClient(kommune="nyborg")
    user_info = client.get_user_by_cpr("1234567890")
    print(user_info)
