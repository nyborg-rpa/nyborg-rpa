import asyncio
import os
import sys
from asyncio import WindowsProactorEventLoopPolicy
from concurrent.futures import ThreadPoolExecutor

import httpx
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

from nyborg_rpa.utils.auth import get_user_login_info


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


class OS2sofdGuiClient(httpx.Client):
    def __init__(
        self,
        *,
        user: str,
        kommune: str,
        **kwargs,
    ):
        """
        Initialize the OS2sofdGuiClient.

        Args:
            kommune: The `{kommune}.sofd.io` domain to connect to.
            user: The username for GUI login.
            kwargs: Extra arguments passed to httpx.Client.
        """

        self.kommune = kommune
        self.user = user
        self.password = get_user_login_info(username=self.user, program="Windows")["password"]
        self.session = self._create_session()

        super().__init__(
            base_url=f"https://{self.kommune}.sofd.io",
            headers=self.session["headers"],
            follow_redirects=False,
            **kwargs,
        )

        for c in self.session["cookies"]:
            self.cookies.set(
                name=c["name"],
                value=c["value"],
                domain=c.get("domain"),
                path=c.get("path"),
            )

    def _create_session(self) -> dict:
        try:
            p = sync_playwright().start()
        except Exception:
            if sys.platform == "win32":
                asyncio.set_event_loop_policy(WindowsProactorEventLoopPolicy())

            with ThreadPoolExecutor(max_workers=1) as executor:
                return executor.submit(self._create_session).result()

        browser = p.chromium.launch(args=["--auth-server-allowlist=_"], headless=True)
        context = browser.new_context(
            http_credentials={"username": self.user, "password": self.password},
        )
        page = context.new_page()
        csrf_token = ""

        def handle_request(request):
            nonlocal csrf_token
            h = request.headers
            if "x-csrf-token" in h:
                csrf_token = h["x-csrf-token"]

        page.on("request", handle_request)

        page.goto(f"https://{self.kommune}.sofd.io/saml/SSO")
        page.goto(f"https://{self.kommune}.sofd.io/ui/orgunit")

        cookies = context.cookies()
        user_agent = page.evaluate("() => navigator.userAgent")

        browser.close()
        p.stop()

        session_info = {}
        session_info["headers"] = {
            "User-Agent": user_agent,
            "x-csrf-token": csrf_token,
            "Content-Type": "application/json",
        }
        session_info["cookies"] = cookies

        return session_info

    def refresh_session(self) -> None:

        resp = self.get("ui/orgunit")
        if resp.status_code == 200:
            return
        elif resp.status_code != 302:
            resp.raise_for_status()

        try:
            p = sync_playwright().start()
        except Exception:
            if sys.platform == "win32":
                asyncio.set_event_loop_policy(WindowsProactorEventLoopPolicy())

            with ThreadPoolExecutor(max_workers=1) as executor:
                return executor.submit(self.refresh_session).result()

        browser = p.chromium.launch(args=["--auth-server-allowlist=_"], headless=True)
        context = browser.new_context(
            http_credentials={"username": self.user, "password": self.password},
        )
        page = context.new_page()
        csrf_token = ""

        def handle_request(request):
            nonlocal csrf_token
            h = request.headers
            if "x-csrf-token" in h:
                csrf_token = h["x-csrf-token"]

        page.on("request", handle_request)

        page.goto(f"https://{self.kommune}.sofd.io/saml/SSO")
        page.goto(f"https://{self.kommune}.sofd.io/ui/orgunit")

        cookies = context.cookies()
        user_agent = page.evaluate("() => navigator.userAgent")

        browser.close()
        p.stop()

        self.headers.update(
            {
                "User-Agent": user_agent,
                "x-csrf-token": csrf_token,
                "Content-Type": "application/json",
            }
        )
        for c in cookies:
            self.cookies.set(
                name=c["name"],
                value=c["value"],
                domain=c.get("domain"),
                path=c.get("path"),
            )

    def get_organization_coreinfo(self, uuid: str) -> dict | None:
        """
        Fetch core organization data.

        Args:
            uuid: The UUID of the organization.

        Returns:
            Dict with organization information if found, otherwise None.
        """

        self.refresh_session()
        resp = self.get(f"ui/orgunit/core/{uuid}/edit")
        resp.raise_for_status()
        html = resp.text
        soup = BeautifulSoup(html, "html.parser")

        organisation_coreinfo = {}
        fields_ids = [
            "sourceName",
            "parentName",
            "parent",
            "shortname",
            "displayName",
            "manager",
            "search_person",
            "cvr",
            "senr",
            "pnr",
            "costBearer",
            "ean",
            "orgUnitType",
            "doNotTransferToFKOrg",
        ]
        find_all_elements = soup.find_all(["select", "input"], id=lambda x: x in fields_ids)
        elements_by_id = {el.get("id"): el for el in find_all_elements}

        for field_id in fields_ids:
            element = elements_by_id.get(field_id)

            match field_id:

                case "doNotTransferToFKOrg":
                    organisation_coreinfo[field_id] = soup.find("input", {"id": "doNotTransferToFKOrgCheckbox"}).has_attr("checked")

                case "orgUnitType":
                    value = element.find("option", selected=True).get("value")
                    organisation_coreinfo[field_id] = value if value != "" else None

                case "cvr" | "senr" | "pnr" | "ean":
                    if element:
                        value = element["value"]
                        organisation_coreinfo[field_id] = int(value) if value != "" else None
                    else:
                        organisation_coreinfo[field_id] = None

                case _:
                    value = element["value"]
                    organisation_coreinfo[field_id] = value if value != "" else None

        return organisation_coreinfo
