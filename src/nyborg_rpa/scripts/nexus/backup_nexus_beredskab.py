import os
import shutil
import time
from datetime import datetime, timedelta
from pathlib import Path

import argh
import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm

from nyborg_rpa.utils.auth import get_user_login_info
from nyborg_rpa.utils.email import send_email
from nyborg_rpa.utils.nexus_client import NexusClient
from nyborg_rpa.utils.pad import dispatch_pad_script

nexus_client: NexusClient


def fetch_calendar_details(calendar_name: str) -> dict:
    """Fetch calendar details for given calendar name."""

    global nexus_client

    # Fetch calendar list and find calendar with given name
    tqdm.write(f"Fetching calendar list to find calendar with name {calendar_name}")
    resp = nexus_client.get("/preferences/CROSS_CITIZEN_CALENDAR")
    resp.raise_for_status()
    calendar_list = resp.json()

    calendar = next((item for item in calendar_list if str(item["name"]).lower() == calendar_name.lower()), None)
    assert calendar, f"Could not find calendar with name {calendar_name}"

    tqdm.write(f"Fetching calendar data for {calendar_name}")
    resp = nexus_client.get(calendar["_links"]["self"]["href"])
    resp.raise_for_status()
    calendar_data = resp.json()

    # Fetch resource ids for calendar, all employess are resources
    tqdm.write(f"Fetching resource ids for calendar {calendar_name}")
    resp = nexus_client.get(calendar_data["_links"]["visits"]["href"], timeout=180)
    resp.raise_for_status()
    visits_data = resp.json()

    # Get list of resource ids, if resource is not visible, add - in front of id
    resource_ids = []
    for resource in visits_data["columnResource"]["resources"]:
        if resource["visible"]:
            resource_ids.append(resource["resourceId"])
        else:
            resource_ids.append("-" + resource["resourceId"])

    # convert list of resource ids to comma separated string
    resource_string = ",".join(resource_ids)
    calendar_data["resourceIds"] = resource_string

    return calendar_data


def fetch_koereliste(calendar_id: str, resource_ids: str, date: str, night: bool = False, save_path: Path = None) -> Path:
    """Fetch calendar for given date and save as PDF file."""

    global nexus_client

    # Convert date to ISO format with correct time
    if night:
        date_iso = pd.Timestamp(date, tz="UTC").replace(hour=23, minute=0, second=0, microsecond=0)
    else:
        date_iso = pd.Timestamp(date, tz="UTC").replace(hour=11, minute=0, second=0, microsecond=0)

    date = date_iso.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    # Create request body for calendar print list endpoint
    body = {
        "filterId": calendar_id,
        "from": date,
        "to": date,
        "calendarMode": "PLANNING_MODE",
        "resourceIds": resource_ids,
        "plannedGrantStatuses": "",
        "registeredGrantStatuses": "",
        "zoomResource": None,
    }

    # Fetch calendar pdf and wait for result to be ready
    tqdm.write(f"Requesting calendar PDF for {calendar_id} on {date}")
    resp = nexus_client.post("calendar/printList/EVENT", json=body)
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
        raise TimeoutError(f"Calendar PDF not ready after 60 seconds for {calendar_id} on {date}")

    # save to pdf file
    if save_path is None:
        save_path = Path("~/Downloads").expanduser().resolve() / f"{calendar_id}.pdf"

    with nexus_client.stream("GET", data["_links"]["result"]["href"], timeout=180) as resp:
        total = int(resp.headers.get("content-length", 0))
        with open(save_path, "wb") as f, tqdm(total=total, unit="B", unit_scale=True) as pbar:
            for chunk in resp.iter_bytes():
                f.write(chunk)
                pbar.update(len(chunk))

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
    emergency_drive_path = Path(os.environ["NEXUS_EMERGENCY_DRIVE"])

    csv_file = Path(os.environ["NEXUS_EMERGENCY_CSV"])
    df = pd.read_csv(csv_file, encoding="utf-8-sig", sep=None, engine="python")

    # Get current date and create folder for today's date in each calendar folder
    now = pd.Timestamp.now()

    error_messages = ""
    # Loop through each row in csv file and fetch calendar for given date and next 2 days, save as PDF file in corresponding folder in emergency drive
    for i, row in tqdm(df.iterrows(), total=len(df), desc="Fetching calendars"):

        koereliste = row["Køreliste"]
        night = str(row["Type"]).strip().lower() == "nat"

        koereliste_path = emergency_drive_path / f"{now:%Y-%m-%d}/{koereliste}"
        koereliste_path.mkdir(parents=True, exist_ok=True)

        try:
            tqdm.write(f"Fetching {koereliste} (Night: {night})")
            calendar_details = fetch_calendar_details(koereliste)
            dates = [now + timedelta(days=i) for i in range(-3, 4)]

            for date in dates:

                destination_file = koereliste_path / f"{koereliste}_{date:%Y-%m-%d}.pdf"
                if destination_file.exists():
                    continue

                fetch_koereliste(calendar_id=calendar_details["id"], resource_ids=calendar_details["resourceIds"], date=date, night=night, save_path=destination_file)
                tqdm.write(f"{destination_file}")

        except Exception as e:
            tqdm.write(f"Error fetching calendar for {koereliste} on {date}: {e}")
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
    for f in emergency_drive_path.iterdir():
        if f.is_dir():
            try:
                folder_date = datetime.strptime(f.name, "%Y-%m-%d")
                if folder_date < (now - timedelta(days=3)):
                    folders.append(f)
            except:
                continue

    for folder in folders:
        print(f"Processing {folder.name}")
        shutil.rmtree(folder)


if __name__ == "__main__":
    dispatch_pad_script(fn=backup_nexus_beredskab)
    # backup_nexus_beredskab(recipients=[""])
