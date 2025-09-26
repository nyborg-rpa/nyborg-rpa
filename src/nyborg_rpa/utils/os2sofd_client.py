import os

import httpx
from dotenv import load_dotenv


class OS2sofdApiClient(httpx.Client):

    def __init__(
        self,
        *,
        kommune: str,
        api_key: str | None = None,
        **kwargs,
    ):
        """
        Initialize the OS2sofdAPIClient.

        Args:
            api_key: API key for SOFD. Defaults to `OS2SOFD_API_KEY` environment variable if not provided.
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
            Dict with user information if found, otherwise None.
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
            Dict with user information if found, otherwise None.
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

    def get_user_by_uuid(self, uuid: str) -> dict | None:
        """
        Fetch user information based on uuid.

        Args:
            uuid: The uuid of the user.

        Returns:
            Dict with user information if found, otherwise None.
        """

        params = {
            "$filter": f"Uuid eq '{uuid}'",
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

        Args:
            uuid: The UUID of the organization.

        Returns:
            Dict with organization information if found, otherwise None.
        """

        params = {
            "$filter": f"uuid eq '{uuid}'",
            # "$select": "Name,Uuid,Manager,Addresses,phones",
            "$expand": "Manager,Addresses,phones,Tags",
        }

        resp = self.get(url="odata/OrgUnits/", params=params)
        resp.raise_for_status()

        data = resp.json()

        return next(iter(data.get("value", [])), None)

    def get_all_organizations(self) -> list[dict]:
        """
        Fetch all organizations.

        Returns:
            List of organizations.
        """

        params = {
            # "$select": "Name,Uuid,Manager,Addresses,phones",
            "$expand": "Manager,Addresses,phones,Tags",
        }

        resp = self.get(url="odata/OrgUnits", params=params)
        resp.raise_for_status()

        data = resp.json()

        return data.get("value", [])

    def get_organization_path(
        self,
        organization: dict | str,
        *,
        separator: str = "/",
    ) -> str:
        """
        Get the full path of an organization by traversing its parent organizations.

        Args:
            organization: The organization dict or its UUID.
            separator: The separator to use between organization names in the path.

        Returns:
            The full path of the organization as a string.
        """

        # fetch organization if only uuid is given
        if isinstance(organization, str):
            organization: dict = self.get_organization_by_uuid(organization)
            if not organization:
                raise ValueError(f"Organization with UUID {organization!r} not found.")

        # traverse parents to build path
        orgs = [organization]
        while parent_uuid := orgs[-1].get("ParentUuid"):
            orgs += [self.get_organization_by_uuid(uuid=parent_uuid)]

        path = separator.join(reversed([org["Name"] for org in orgs]))

        return path

    def post_organization_manager(
        self,
        *,
        organization_uuid: str,
        user_uuid: str,
    ):
        """
        Assign a user as the manager of an organization.

        Args:
            organization_uuid: The UUID of the organization.
            user_uuid: The UUID of the user to assign as manager.
        """

        resp = self.post(
            url="api/manager/orgUnitManagers",
            json={
                "orgUnitUuid": organization_uuid,
                "managerUuid": user_uuid,
            },
        )

        if str(resp.status_code).startswith(("4", "5")):
            resp.raise_for_status()

    def patch_organization(
        self,
        *,
        uuid: str,
        json: dict,
    ) -> bool:
        """
        Update organization details and return whether data was changed.

        Args:
            uuid: The UUID of the organization.
            json: The JSON data to update the organization with.

        Returns:
            Boolean indicating if data was changed.
        """

        resp = self.patch(
            url=f"api/v2/orgUnits/{uuid}",
            json=json,
        )

        if str(resp.status_code).startswith(("4", "5")):
            resp.raise_for_status()

        did_data_change = resp.status_code in {200, 204}

        return did_data_change
