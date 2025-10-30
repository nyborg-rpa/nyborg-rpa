import asyncio
import sys
import urllib
from asyncio import WindowsProactorEventLoopPolicy
from concurrent.futures import ThreadPoolExecutor

import httpx
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright


class TunstallGuiClient(httpx.Client):
    def __init__(
        self,
        *,
        user: str,
        password: str,
        **kwargs,
    ):
        """
        Initialize the OS2sofdGuiClient.

        Args:
            kommune: The `{kommune}.sofd.io` domain to connect to.
            user: The username for GUI login.
            kwargs: Extra arguments passed to httpx.Client.
        """

        self.user = user
        self.password = password
        self.session = self._create_session()

        super().__init__(
            base_url=f"https://045001.carehosting.dk",
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

        page.goto(f"https://045001.carehosting.dk")

        page.wait_for_timeout(5000)

        cookies = context.cookies()
        user_agent = page.evaluate("() => navigator.userAgent")

        browser.close()
        p.stop()

        session_info = {}
        session_info["headers"] = {
            "User-Agent": user_agent,
            "Content-Type": "application/x-www-form-urlencoded",
        }
        session_info["cookies"] = cookies

        return session_info

    def search_user(self, *, role: str, department: str = "Nyborg", employee_text: str | None = None) -> dict:
        resp = self.get("personnel_search.aspx")
        resp.raise_for_status()

        html = resp.text
        soup = BeautifulSoup(html, "html.parser")
        find_all_elements = soup.find_all(["select", "input"])
        elements_by_id = {el.get("name"): el.find_all("option") if el.name == "select" else el.get("value") for el in find_all_elements}

        payload = elements_by_id
        payload.pop("_ctl0:_ctl0:PageContent:AspContent:btnReset", None)
        payload.pop("_ctl0:_ctl0:PageContent:AspContent:chkPrevMb", None)
        payload.pop("_ctl0:_ctl0:PageContent:AspContent:chkTerm", None)
        payload.pop("_ctl0:_ctl0:PageContent:AspContent:cboComp", None)
        payload["_ctl0:_ctl0:PageContent:AspContent:cboRole"] = next(opt.get("value") for opt in payload["_ctl0:_ctl0:PageContent:AspContent:cboRole"] if role in opt.text)
        payload["_ctl0:_ctl0:PageContent:AspContent:cboDept"] = next(opt.get("value") for opt in payload["_ctl0:_ctl0:PageContent:AspContent:cboDept"] if department in opt.text)
        payload["_ctl0:_ctl0:PageContent:AspContent:txtEmployeeID"] = employee_text

        payload_clean = {k: v for k, v in payload.items() if v is not None}
        payload_encoded = urllib.parse.urlencode(payload_clean)

        resp = self.post(url="personnel_search.aspx", data=payload_encoded)
        resp.raise_for_status()

        html = resp.text
        soup = BeautifulSoup(html, "html.parser")

        # Get headers names and a link in current order
        headers = [th.get_text(strip=True) for th in soup.find_all("th")]
        headers.insert(1, "Link")

        # Fetch all data from html resp
        rows = []
        for tr in soup.find_all("tr"):
            tds = tr.find_all("td")
            if tds:
                row = []
                for idx, td in enumerate(tds):
                    a_tag = td.find("a")
                    if a_tag:
                        row.append(a_tag.get_text(strip=True))
                        row.append(a_tag.get("href"))
                    else:
                        row.append(td.get_text(strip=True))
                rows.append(row)

        result = [dict(zip(headers, row)) for row in rows]

        return result
