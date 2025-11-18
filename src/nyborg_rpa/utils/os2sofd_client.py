import asyncio
import os
import re
import sys
from asyncio import WindowsProactorEventLoopPolicy
from concurrent.futures import ThreadPoolExecutor
from typing import NotRequired, TypedDict

import httpx
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import sync_playwright
from tqdm import tqdm

from nyborg_rpa.utils.git import latest_commit_hash


class OrgCoreInfo(TypedDict):
    sourceName: str | None
    parentName: str | None
    parent: str | None
    shortname: str | None
    displayName: str | None
    manager: str | None
    search_person: str | None
    cvr: int | None
    senr: int | None
    pnr: int | None
    costBearer: str | None
    ean: int | None
    orgUnitType: str | None
    doNotTransferToFKOrg: bool
    inheritedFkOrg: NotRequired[bool]


class OrgAddress(TypedDict):
    id: str
    street: str
    localname: str
    postalCode: str
    city: str
    country: str
    returnAddress: bool
    prime: bool
    master: NotRequired[str]


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
            cpr: The CPR number of the user on the format DDMMYYXXXX.

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
        password: str,
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
        self.password = password

        super().__init__(
            base_url=f"https://{self.kommune}.sofd.io",
            follow_redirects=False,
            **kwargs,
        )

        # verify that backend endpoint files have not been modified
        self.verify_endpoint_hashes()

    @property
    def endpoints(self) -> list[dict]:
        return [
            {
                "fn": "public String getCoreFragment",
                "permalink": "https://github.com/OS2sofd/os2sofd/blob/8f3e1efec8201bf4f437c663e1e28fb377cf65a6/ui/src/main/java/dk/digitalidentity/sofd/controller/mvc/OrgUnitController.java#L720",
                "dependees": [self.get_organization_coreinfo],
            },
            {
                "fn": "public class OrgUnitCoreInfo",
                "permalink": "https://github.com/OS2sofd/os2sofd/blob/cfb75fe2819ee4e67cf12001f3401c57b809ef7b/ui/src/main/java/dk/digitalidentity/sofd/controller/rest/model/OrgUnitCoreInfo.java#L10",
                "dependees": [self.get_organization_coreinfo, self.post_organization_coreinfo],
            },
            {
                "fn": "public HttpEntity<?> updateCoreInformation",
                "permalink": "https://github.com/OS2sofd/os2sofd/blob/cfb75fe2819ee4e67cf12001f3401c57b809ef7b/ui/src/main/java/dk/digitalidentity/sofd/controller/rest/OrgUnitRestController.java#L350",
                "dependees": [self.post_organization_coreinfo],
            },
            {
                "fn": "public String getPostsTab",
                "permalink": "https://github.com/OS2sofd/os2sofd/blob/8f3e1efec8201bf4f437c663e1e28fb377cf65a6/ui/src/main/java/dk/digitalidentity/sofd/controller/mvc/OrgUnitController.java#L752",
                "dependees": [self.get_organization_addresses],
            },
            {
                "fn": "public ResponseEntity<?> editOrCreatePost",
                "permalink": "https://github.com/OS2sofd/os2sofd/blob/cfb75fe2819ee4e67cf12001f3401c57b809ef7b/ui/src/main/java/dk/digitalidentity/sofd/controller/rest/OrgUnitRestController.java#L564",
                "dependees": [self.edit_or_create_organization_address],
            },
        ]

    @property
    def login_url(self) -> str:
        return f"{self.base_url}/saml/SSO"

    def login(self) -> None:
        """Login to OS2sofd GUI and update session headers and cookies."""

        tqdm.write(f"Logging in to {self.login_url!r} as {self.user!r}...")
        self.session = self._create_session()
        self.headers.update(self.session["headers"])
        for cookie in self.session["cookies"]:
            self.cookies.set(cookie["name"], cookie["value"], domain=cookie.get("domain"), path=cookie.get("path"))

    def request(self, method: str, url: str, **kwargs) -> httpx.Response:

        resp = super().request(method, url, **kwargs)

        # if we are redirected to login page, use login and retry request
        if resp.status_code == 302 and resp.headers.get("Location") == self.login_url:
            self.login()
            resp = super().request(method, url, **kwargs)

        return resp

    def verify_endpoint_hashes(self) -> None:
        """Verify that the backend endpoint files have not been modified."""

        tqdm.write("Verifying backend endpoint file hashes...")
        modified_endpoints = []
        for endpoint in self.endpoints:
            sha, path = re.match(r".*/([a-f0-9]{40})/(.+)#L\d+$", endpoint["permalink"]).groups()
            current_sha = latest_commit_hash(repository="OS2sofd/os2sofd", path=path, sha="master")
            modified_endpoints += [endpoint] if current_sha != sha else []

        if modified_endpoints:
            fns = [e["fn"] for e in modified_endpoints]
            raise ValueError(f"Endpoints in OS2sofdGuiClient have been modified: {fns}.")

    def _create_session(self) -> dict:

        try:
            p = sync_playwright().start()

        except PlaywrightError:

            if sys.platform == "win32":
                asyncio.set_event_loop_policy(WindowsProactorEventLoopPolicy())

            with ThreadPoolExecutor(max_workers=1) as executor:
                return executor.submit(self._create_session).result()

        browser = p.chromium.launch(args=["--auth-server-allowlist=_"], headless=True)
        http_credentials = {"username": self.user, "password": self.password}
        context = browser.new_context(http_credentials=http_credentials)
        page = context.new_page()

        # login using SSO (http_credentials in context)
        page.goto(str(self.login_url))

        # extract csrf token, cookies, and user-agent
        page.goto(str(self.base_url))
        user_agent = page.evaluate("() => navigator.userAgent")
        csrf_token = page.eval_on_selector('meta[name="_csrf"]', "el => el.content")
        cookies = context.cookies()

        browser.close()
        p.stop()

        session = {
            "cookies": cookies,
            "headers": {
                "x-csrf-token": csrf_token,
                "User-Agent": user_agent,
                "Content-Type": "application/json",
            },
        }

        return session

    def get_organization_coreinfo(self, *, uuid: str, include_inherited_fkorg: bool = False) -> OrgCoreInfo:
        """
        Fetch core organization data.

        Args:
            uuid: The UUID of the organization.
            include_inherited_fkorg: Whether to check for inherited FK organization exemption.

        Returns:
            Dict with organization information if found, otherwise None.
        """

        resp = self.get(f"ui/orgunit/core/{uuid}/edit")
        resp.raise_for_status()
        html = resp.text
        soup = BeautifulSoup(html, "html.parser")

        fields = list(OrgCoreInfo.__annotations__)
        all_elements = soup.find_all(["select", "input"], id=lambda x: x in fields)
        elements_by_id = {el.get("id"): el for el in all_elements}

        org_coreinfo = {}
        for field in fields:

            element = elements_by_id.get(field)

            match field:

                case "doNotTransferToFKOrg":
                    org_coreinfo[field] = soup.find("input", {"id": "doNotTransferToFKOrgCheckbox"}).has_attr("checked")

                case "orgUnitType":
                    value = element.find("option", selected=True).get("value")
                    org_coreinfo[field] = value if value != "" else None

                case "cvr" | "senr" | "pnr" | "ean":
                    if element:
                        value = element["value"]
                        org_coreinfo[field] = int(value) if value != "" else None
                    else:
                        org_coreinfo[field] = None

                case "inheritedFkOrg":
                    if include_inherited_fkorg:
                        org_coreinfo["inheritedFkOrg"] = "Enheden er undtaget pga. nedarvning" in html

                case _:
                    value = element["value"]
                    org_coreinfo[field] = value if value != "" else None

        return org_coreinfo

    def post_organization_coreinfo(self, *, uuid: str, data: OrgCoreInfo) -> None:
        """
        Update organization coreinfo details and return whether data was changed.

        Args:
            uuid: The UUID of the organization.
            data: The JSON data to update the organization with; must include all required keys.
        """

        required_keys = set(OrgCoreInfo.__required_keys__)
        if wrong_keys := set(data.keys()) ^ required_keys:
            raise ValueError(f"Data contains wrong or missing keys: {wrong_keys}. Expected: {required_keys}")

        resp = self.post(f"rest/orgunit/{uuid}/update/coreInfo", json=data)
        resp.raise_for_status()

    def get_organization_addresses(self, uuid: str) -> list[OrgAddress]:
        """
        Fetch organization addresses.

        Args:
            uuid: The UUID of the organization.

        Returns:
            List of addresses.
        """

        resp = self.get(f"ui/orgunit/postsTab/{uuid}")
        resp.raise_for_status()
        html = resp.text
        soup = BeautifulSoup(html, "html.parser")

        address_elements = soup.find_all("a", {"onclick": "openPostEditModal(this);"})

        addresses = []
        for el in address_elements:
            address = {k: el.get(f"data-{k.lower()}") for k in OrgAddress.__annotations__.keys()}
            address = {k: (v.lower() == "true" if v in ["true", "false"] else v) for k, v in address.items()}
            addresses += [address]

        return addresses

    def edit_or_create_organization_address(self, *, uuid: str, address: OrgAddress) -> None:
        """
        Update organization address.

        Args:
            uuid: The UUID of the organization.
            address: The address data to update or create.
        """

        if not isinstance(address, dict):
            raise TypeError(f"json must be a dict, but got {type(address).__name__}")

        # remove 'master' key if present just to be safe
        if "master" in address:
            tqdm.write("Warning: 'master' key in address will be ignored when posting to SOFD GUI.")
            del address["master"]

        required_keys = set(OrgAddress.__required_keys__)
        if wrong_keys := set(address.keys()) ^ required_keys:
            raise ValueError(f"Address contains wrong or missing keys: {wrong_keys}. Expected: {required_keys}")

        headers = {**self.headers, "Uuid": uuid}
        resp = self.post("https://nyborg.sofd.io/rest/orgunit/editOrCreatePost", json=address, headers=headers)
        resp.raise_for_status()
