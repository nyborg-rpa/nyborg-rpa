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

    def get_user_by_username(self, username: str) -> dict | None:
        """
        Fetch user information based on username.

        Args:
            username: The username of the user.

        Returns:
            dict: User information if found, otherwise None.
        """
        params = {
            "$filter": (f"Users/any(u: u/UserId eq '{username}') " f"or DisabledUsers/any(d: d/UserId eq '{username}')"),
            "$top": 1,
            "$expand": "Affiliations,Users,Photo,Phones,Children,AuthorizationCodes,Substitutes,DisabledUsers",
        }

        resp = self.get(url="odata/Persons", params=params)
        resp.raise_for_status()

        data = resp.json()
        user = next(iter(data.get("value", [])), None)

        return user

    def get_organization_by_uuid(self, uuid: str) -> dict | None:
        """
        Fetch all organization data.

        Returns:
            list[dict]: List of organizations.
        """
        params = {
            "$filter": f"uuid eq '{uuid}'",
            # "$select": "Name,Uuid,Manager,Addresses,phones",
            "$expand": "Manager,Addresses,phones",
        }

        resp = self.get(url="odata/OrgUnits/", params=params)
        resp.raise_for_status()

        data = resp.json()

        return next(iter(data.get("value", [])), None)

    def get_all_organizations(self) -> list[dict]:
        """
        Fetch all organizations.

        Returns:
            list[dict]: List of organizations.
        """
        params = {
            # "$select": "Name,Uuid,Manager,Addresses,phones",
            "$expand": "Manager,Addresses,phones",
        }

        resp = self.get(url="odata/OrgUnits", params=params)
        resp.raise_for_status()

        data = resp.json()

        return data.get("value", [])

    def post_organization_manager(self, organization_uuid: str, user_uuid: str):
        """
        Assign a user as the manager of an organization.

        Args:
            organization_uuid: The UUID of the organization.
            user_uuid: The UUID of the user to assign as manager.

        Returns:
            dict: Response from the server.
        """

        resp = self.post(
            url="api/manager/orgUnitManagers",
            json={
                "orgUnitUuid": organization_uuid,
                "managerUuid": user_uuid,
            },
        )
        resp.raise_for_status()

    def patch_organization(self, uuid: str, json: dict):
        """
        Assign new primary address of an organization.

        Args:
            organization_uuid: The UUID of the organization.
            json: dict of changes
        """

        resp = self.patch(
            url=f"api/v2/orgUnits/{uuid}",
            json=json,
        )
        if resp.status_code == 304:
            status = False
        elif resp.status_code == 200:
            status = True
        else:
            resp.raise_for_status()

        return status


if __name__ == "__main__":
    # Example usage
    client = OS2sofdClient(kommune="nyborg")
