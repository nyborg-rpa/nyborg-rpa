import contextlib
import os
import shutil
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import argh
import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm

from nyborg_rpa.utils.auth import get_user_login_info
from nyborg_rpa.utils.email import send_email
from nyborg_rpa.utils.nexus_client import NexusClient
from nyborg_rpa.utils.pad import dispatch_pad_script

nexus_client: NexusClient


def fetch_calendars() -> list[dict]:
    """Fetch calendar details for given calendar name."""

    # Fetch calendar list and find calendar with given name
    resp = nexus_client.get("/preferences/CROSS_CITIZEN_CALENDAR")
    resp.raise_for_status()
    calendars = resp.json()

    return calendars


def fetch_koereliste(*, calendar: dict, date: datetime, save_path: Path) -> Path:
    """Fetch calendar for given date and save as PDF file."""

    date = date.astimezone(ZoneInfo("UTC"))

    # Fetch resource ids active on given date for calendar, resource ids are needed to fetch calendar pdf
    tqdm.write(f"[{calendar['name'].upper()}] Fetching resource ids for calendar {calendar['name']}, date: {date}")
    resp = nexus_client.post(
        url=f"calendar/events/criteria/EVENT/{calendar['id']}",
        params={
            "calendarMode": "PLANNING_MODE",
            "from": f"{date:%Y-%m-%dT%H:%M:%S.000Z}",
            "to": f"{date:%Y-%m-%dT%H:%M:%S.000Z}",
            "plannedGrantStatuses": "",
            "registeredGrantStatuses": "",
            "showPermanentResources": "false",
            "showResourcesWithEvents": "true",
            "showResourcesWithShifts": "false",
        },
        timeout=180,
    )
    resp.raise_for_status()
    resource_data = resp.json()

    # Get list of resource ids, if resource is not visible, add - in front of id
    resource_ids = [r["resourceId"] for r in resource_data["columnResource"]["resources"] if r["visible"]]
    resource_string = ",".join(resource_ids)

    # Fetch calendar pdf and wait for result to be ready
    tqdm.write(f"[{calendar['name'].upper()}] Requesting calendar PDF for {calendar['name']} on {date}")

    resp = nexus_client.post(
        url="calendar/printList/EVENT",
        json={
            "filterId": calendar["id"],
            "from": f"{date:%Y-%m-%dT%H:%M:%S.000Z}",
            "to": f"{date:%Y-%m-%dT%H:%M:%S.000Z}",
            "calendarMode": "PLANNING_MODE",
            "resourceIds": resource_string,
            "plannedGrantStatuses": "",
            "registeredGrantStatuses": "",
            "zoomResource": None,
        },
    )
    resp.raise_for_status()
    data = resp.json()

    t1 = datetime.now()
    while (datetime.now() - t1).seconds <= 60:
        if data["resultReady"] == True:
            break
        time.sleep(1)
        resp = nexus_client.get(data["_links"]["self"]["href"])
        data = resp.json()
    else:  # if we exit the loop without breaking, it means the result is not ready after 60 seconds
        raise TimeoutError(f"Calendar PDF not ready after 60 seconds for {calendar['name']} on {date}")

    # download pdf file
    tqdm.write(f"[{calendar['name'].upper()}] Downloading calendar PDF for {calendar['name']} on {date}")
    resp = nexus_client.get(data["_links"]["result"]["href"], timeout=300)
    resp.raise_for_status()

    # save to pdf file
    save_path.write_bytes(resp.content)

    return save_path


@argh.arg("--recipients", help="List of email recipients to send report to in case of error", nargs="*")
def backup_nexus_beredskab(*, recipients: list[str]):
    """Fetch calendar for given date and next 2 days and save as PDF file."""
    load_dotenv(dotenv_path=r"J:\RPA\.baseflow\.env", override=True)
    global nexus_client

    # Get login info and create Nexus client
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

    # load emergency drive path from env variable and read csv file with calendar names and types
    emergency_path = Path(os.environ["NEXUS_EMERGENCY_DRIVE"])
    emergency_calendar_path = emergency_path / "Kørelister"
    csv_file = emergency_path / "Setting/Kørelister.csv"
    df = pd.read_csv(csv_file, encoding="utf-8-sig", sep=None, engine="python")

    # Get current date and create folder for today's date in each calendar folder
    now = datetime.now(tz=ZoneInfo("Europe/Copenhagen"))

    # Fetch all calendars
    calendars = fetch_calendars()

    # Loop through each row in csv file and fetch calendar for given date and next 2 days, save as PDF file in corresponding folder in emergency drive
    error_messages = ""
    for i, row in tqdm(df.iterrows(), total=len(df), desc="Fetching calendars"):

        koereliste = row["Køreliste"]
        night = str(row["Type"]).strip().lower() == "nat"

        koereliste_path = emergency_calendar_path / f"{now:%Y-%m-%d}/{koereliste}"
        koereliste_path.mkdir(parents=True, exist_ok=True)

        try:
            tqdm.write(f"Fetching {koereliste} ({night=})...")

            calendar = next((item for item in calendars if str(item["name"]).lower() == koereliste.lower()), None)
            assert calendar, f"Could not find calendar with name {koereliste}"

            dates = []
            for i in range(-3, 4):
                hour = 12 if night else 00
                dates += [(now + timedelta(days=i)).replace(hour=hour, minute=0, second=0, microsecond=0)]

            for date in dates:
                destination_file = koereliste_path / f"{koereliste}_{date:%Y-%m-%d}.pdf"
                if destination_file.exists():
                    continue

                fetch_koereliste(calendar=calendar, date=date, save_path=destination_file)
                tqdm.write(f"[{koereliste.upper()}] Saved {destination_file}")

        except Exception as e:
            tqdm.write(f"[{koereliste.upper()}] Error fetching calendar for {koereliste} on {date}: {e}")
            error_messages += f"<p>Error fetching calendar for {koereliste} on {date}: {e}</p>"

    # Send email if there were any errors
    if error_messages:
        typography_style = "font-family: Arial, sans-serif; font-size: 12px"
        body = f"""<!DOCTYPE html>
        <html>
        <body style="margin:0; padding:0; {typography_style}; line-height:1.4;">
        {error_messages}
        <p>Venlig hilsen,<br>Robotten</p>
        </body>
        </html>"""
        send_email(
            sender=os.environ["MS_MAILBOX"],
            recipients=recipients,
            subject="Beredskabsdrev - Fejl ved hentning af kalendere",
            body=body,
        )

    # get all folder in emergency drive path except for folders that are newer than 3 days and delete them, shall also handle case where folder name is not a date
    folders = []
    for f in emergency_calendar_path.iterdir():
        if f.is_dir():
            with contextlib.suppress(Exception):
                folder_date = datetime.strptime(f.name, "%Y-%m-%d")
                if folder_date < (now - timedelta(days=3)):
                    folders.append(f)

    for folder in folders:
        print(f"Processing {folder.name}")
        shutil.rmtree(folder)


if __name__ == "__main__":
    dispatch_pad_script(fn=backup_nexus_beredskab)
    # backup_nexus_beredskab(recipients=[""])
