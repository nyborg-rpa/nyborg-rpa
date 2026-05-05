import base64
import hashlib
import hmac
import os
import struct
import time
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Literal

import argh
from dotenv import load_dotenv
from playwright.sync_api import Page, sync_playwright

from nyborg_rpa.utils.auth import get_user_login_info
from nyborg_rpa.utils.pad import dispatch_pad_script


def generate_totp(secret: str) -> str:

    # TOTP parameters
    interval = 30
    digits = 6

    # decode the base32 secret key and get the current time counter
    key = base64.b32decode(secret, casefold=True)
    current_time = int(time.time())
    counter = current_time // interval

    # create the HMAC-SHA1 hash of the counter using the secret key
    msg = struct.pack(">Q", counter)
    h = hmac.new(key, msg, hashlib.sha1).digest()

    offset = h[-1] & 0x0F
    binary = (h[offset] & 0x7F) << 24 | (h[offset + 1] & 0xFF) << 16 | (h[offset + 2] & 0xFF) << 8 | (h[offset + 3] & 0xFF)

    otp = binary % (10**digits)
    code = str(otp).zfill(digits)

    return code


def handle_kmd_i2_mfa(page: Page):

    page.goto(url="https://institution.kmd.dk/Caseworker#/dashboard", wait_until="networkidle")
    page.get_by_text("FÆLLESKOMMUNAL ADGANGSSTYRING").click()
    page.wait_for_load_state("networkidle")
    if page.query_selector("#SelectedAuthenticationUrl"):
        page.select_option("#SelectedAuthenticationUrl", "Nyborg Kommune")
        page.click("#btnOK")


def handle_nexus_mfa(page: Page):

    page.goto(url="https://nyborg.nexus.kmd.dk/", wait_until="networkidle")
    page.get_by_text("Log ind for medarbejder").click()
    page.wait_for_load_state("networkidle")
    if page.query_selector("#SelectedAuthenticationUrl"):
        page.select_option("#SelectedAuthenticationUrl", "Nyborg Kommune")
        page.click("#btnOK")


def handle_nexus_review_mfa(page: Page):

    page.goto(url="https://nyborg.nexus-review.kmd.dk/", wait_until="networkidle")
    page.get_by_text("Log ind for medarbejder").click()
    page.wait_for_load_state("networkidle")
    if page.query_selector("#SelectedAuthenticationUrl"):
        page.select_option("#SelectedAuthenticationUrl", "Nyborg Kommune")
        page.click("#btnOK")


def handle_fasit_mfa(page: Page):

    page.goto(url="https://login.fasit.dk/nyborg/kombit/", wait_until="networkidle")
    if page.query_selector("#SelectedAuthenticationUrl"):
        page.select_option("#SelectedAuthenticationUrl", "Nyborg Kommune")
        page.click("#btnOK")


def handle_kp_mfa(page: Page):

    page.goto(url="https://fagsystem.kommunernespensionssystem.dk", wait_until="networkidle")
    if page.query_selector("#SelectedAuthenticationUrl"):
        page.select_option("#SelectedAuthenticationUrl", "Nyborg Kommune")
        page.click("#btnOK")


def handle_ksd_mfa(page: Page):

    page.goto(url="https://ksdp.dk/start", wait_until="networkidle")

    if page.query_selector("#SelectedAuthenticationUrl"):
        page.select_option("#SelectedAuthenticationUrl", "Nyborg Kommune")
        page.click("#btnOK")


def handle_dubu_mfa(page: Page):

    page.goto(url="https://www.dubu.dk", wait_until="networkidle")
    if page.query_selector("#SelectedAuthenticationUrl"):
        page.select_option("#SelectedAuthenticationUrl", "Nyborg Kommune")
        page.click("#btnOK")


def handle_nfs_mfa(page: Page):

    page.goto(url="https://adfs.egki.dk/450/nfs", wait_until="networkidle")
    if page.query_selector("#SelectedAuthenticationUrl"):
        page.select_option("#SelectedAuthenticationUrl", "Nyborg Kommune")
        page.click("#btnOK")


def handle_sapa_mfa(page: Page):

    page.goto(url="https://sapaoverblik.dk/", wait_until="networkidle")
    if page.query_selector("#SelectedAuthenticationUrl"):
        page.select_option("#SelectedAuthenticationUrl", "Nyborg Kommune")
        page.click("#btnOK")


def handle_sd_mfa(page: Page):

    page.goto(url="https://www.silkeborgdata.dk/start/", wait_until="networkidle")
    page.click("#arbejdspladsButton")
    frame = page.frame_locator("#iframe-oiosaml")
    frame.locator("#oiosaml-idp").select_option(label="Nyborg Kommune")
    frame.locator("#oiosaml-login-btn").click()


def handle_prisme365_mfa(page: Page):

    page.goto(url="https://ax.prisme-365.dk/namespaces/AXSF/?cmp=NYK", wait_until="networkidle")


def handle_ky_mfa(page: Page):

    page.goto(url="https://fs0450.fs.kommunernesydelsessystem.dk/ky-fagsystem/", wait_until="networkidle")
    if page.query_selector("#SelectedAuthenticationUrl"):
        page.select_option("#SelectedAuthenticationUrl", "Nyborg Kommune")
        page.click("#btnOK")


def handle_test_mfa(page: Page):

    page.goto(url="https://login.nyborg.dk/selvbetjening", wait_until="networkidle")


@argh.arg("--system", help='"kmd_i2", "nexus", "nexus_review", "fasit", "kp", "ksd", "sd", "prisme365", "sapa", "ky", "test"')
@argh.arg("--username", help="Robot username to use for login")
def mfa_login(*, system: Literal["kmd_i2", "nexus", "nexus_review", "fasit", "kp", "ksd", "sd", "prisme365", "sapa", "ky", "test"], username: str):

    print(f"Starting MFA login for system: {system} with username: {username}")
    print("Loading environment variables and user login info...")
    load_dotenv(dotenv_path=r"\\nbfil2\rpa\RPA\.baseflow\.env", override=True)
    password = get_user_login_info(username=username, program="Windows")["password"]

    print("Launching Playwright browser for MFA login...")
    with TemporaryDirectory(prefix="playwright_") as tmp_dir:

        with sync_playwright() as pw:

            pad_ext_id = "kagpabjoboikccfdghpdlaaopmgpgfdc"
            pad_ext_path = next(d for d in (Path.home() / f"AppData/Local/Microsoft/Edge/User Data/Default/Extensions/{pad_ext_id}").iterdir() if d.is_dir())

            print("Launching Playwright...")
            context = pw.chromium.launch_persistent_context(
                user_data_dir=tmp_dir,
                channel="chromium",
                headless=False,
                http_credentials={
                    "username": username,
                    "password": password,
                },
                no_viewport=True,
                args=[
                    "--start-maximized",
                    "--auth-server-allowlist=_",
                    f"--disable-extensions-except={pad_ext_path}",
                    f"--load-extension={pad_ext_path}",
                    "--no-first-run",
                    "--no-default-browser-check",
                    "--disable-search-engine-choice-screen",
                ],
            )

            page = context.pages[0]

            # set download path
            page.on("download", lambda download: download.save_as(Path("~/Downloads").expanduser() / download.suggested_filename))

            all_fns_in_file = [obj for _, obj in globals().items() if callable(obj)]
            fn = next(fn for fn in all_fns_in_file if fn.__name__ == f"handle_{system}_mfa")
            fn(page)

            # check for login form and fill if it exists
            page.wait_for_load_state("networkidle")
            if page.query_selector("#username"):
                page.fill("#username", username)
                page.fill("#password", password)
                page.locator("button:has-text('Login')").click()
                page.wait_for_load_state("networkidle")

                if page.query_selector(".iCheck-helper") and page.query_selector(".iCheck-helper").is_visible():
                    page.click(".iCheck-helper")
                    page.click("#buttonAccept")
                    page.wait_for_load_state("networkidle")

            # check for mfa code input and fill it if it exists
            if page.query_selector("#mfaCode"):
                secret = os.environ[f"MFA_SECRET_{username.upper()}"]
                mfa_code = generate_totp(secret=secret)
                page.fill("#mfaCode", mfa_code)
                page.locator("#mfaCode").blur()
                page.click("#loginBtn")
                page.wait_for_load_state("networkidle")

            context.wait_for_event("close", timeout=0)

        print("Playwright browser closed.")


if __name__ == "__main__":
    dispatch_pad_script(fn=mfa_login)
