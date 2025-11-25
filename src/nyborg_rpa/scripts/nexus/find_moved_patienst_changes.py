import os

import argh
from dotenv import load_dotenv

from nyborg_rpa.utils.auth import get_user_login_info
from nyborg_rpa.utils.email import send_email
from nyborg_rpa.utils.nexus_client import NexusClient
from nyborg_rpa.utils.pad import dispatch_pad_script

nexus_client: NexusClient
nexus_environment: str


def fetch_moved_patients() -> set[str]:

    global nexus_client

    # find list "Borger fraflyttet kommunen med hjælpemiddel"
    resp = nexus_client.get(url="preferences/CITIZEN_LIST")
    resp.raise_for_status()
    list_url = next(item for item in resp.json() if item["name"] == "Borger fraflyttet kommunen med hjælpemiddel")["_links"]["self"]["href"]

    print("Fetching moved patients from Nexus...")
    resp = nexus_client.get(url=list_url)
    resp.raise_for_status()
    content_url = resp.json()["_links"]["content"]["href"]

    resp = nexus_client.get(url=content_url, timeout=180)
    resp.raise_for_status()
    patients_view = resp.json()

    print("Extracting moved patients...")
    moved_patients = set()
    for page in patients_view["pages"]:
        data: str = page["_links"]["patientData"]["href"]
        moved_patients |= set(data.replace("https://nyborg.nexus.kmd.dk:443/api/core/mobile/nyborg/v2/patients?ids=", "").split(","))

    return moved_patients


def generate_report_email(moved_patients: set[str]) -> str:

    global nexus_environment

    typography_style = "font-family: Arial, sans-serif; font-size: 12px"
    body = f"""<!DOCTYPE html>
    <html>
    <body style="margin: 0; padding: 0; {typography_style};">
    <p>Følgende borgere er nye på listen "Borger fraflyttet kommunen med hjælpemiddel":</p>
    <table border="1" cellpadding="4" cellspacing="0" width="100%" style="border-collapse: collapse; {typography_style};">
        <tr style="background-color: #dddddd; font-weight: bold;">
        <td>Borgere</td>
        </tr>"""

    for patient_id in moved_patients:
        body += f"""
        <tr style="background-color: #f0f0f0; font-weight: bold;">
        <td><a href="https://nyborg.{nexus_environment}.kmd.dk/citizen/{patient_id}" style="color: #0000EE;">{patient_id}</a></td>
        </tr>"""

    body += """
    </table>
    <p>Venlig hilsen,<br>Robotten</p>
    </body>
    </html>"""

    return body


@argh.arg("--recipients", help="List of email recipients for the report.", nargs="*")
def find_moved_patients_changes(*, recipients: list[str]):
    """Find moved patients and send email report if there are new ones."""

    global nexus_client, nexus_environment

    # initialize Nexus client
    login_info = get_user_login_info(
        username="API",
        program="Nexus-Drift",
    )

    nexus_environment = "nexus"
    nexus_client = NexusClient(
        client_id=login_info["username"],
        client_secret=login_info["password"],
        instance="nyborg",
        enviroment=nexus_environment,
    )

    # load previously moved patients
    with open("src/nyborg_rpa/scripts/nexus/previous_moved_patients.txt", "r") as f:
        prev_moved_patients = set([line.strip() for line in f])

    # find currently moved patients
    moved_patients = fetch_moved_patients()
    new_moved_patients = moved_patients - prev_moved_patients

    if new_moved_patients:

        print(f"Found {len(new_moved_patients)} new moved patients, sending email...")
        load_dotenv(override=True)
        send_email(
            sender=os.environ["MS_MAILBOX"],
            recipients=recipients,
            subject="Rapport: Nye fraflyttede borgere med hjælpemiddel",
            body=generate_report_email(new_moved_patients),
        )

        # save currently moved patients for next run
        with open("src/nyborg_rpa/scripts/nexus/previous_moved_patients.txt", "w") as f:
            for p in moved_patients:
                f.write(p + "\n")


if __name__ == "__main__":
    dispatch_pad_script(fn=find_moved_patients_changes)
