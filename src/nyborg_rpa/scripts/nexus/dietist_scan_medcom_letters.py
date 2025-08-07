import base64
import os
from collections import defaultdict

import pandas as pd
from dotenv import load_dotenv
from office365.graph_client import GraphClient
from tqdm.auto import tqdm

from nyborg_rpa.utils.auth import get_user_login_info
from nyborg_rpa.utils.nexus_client import NexusClient
from nyborg_rpa.utils.send_mail import send_email

sharepoint_client: GraphClient
nexus_environment: str
nexus_client: NexusClient

EXPECTED_DISTRICTS: tuple[dict] = (
    {"name": "Distrikt Aften By", "active": False},
    {"name": "Distrikt Aften Land", "active": False},
    {"name": "Distrikt Egepark", "active": True},
    {"name": "Distrikt Egevang", "active": True},
    {"name": "Distrikt Nat", "active": False},
    {"name": "Distrikt Rosengård", "active": True},
    {"name": "Distrikt Svanedam Vest", "active": True},
    {"name": "Distrikt Svanedam Øst", "active": True},
    {"name": "E-distrikt.", "active": False},
    {"name": "Private leverandører", "active": False},
)


def fetch_medcom_letters(activity_name: str) -> list[dict]:

    to_date = pd.Timestamp.now()
    from_date = to_date - pd.Timedelta(days=30)

    print(f"Fetching Medcom letters for activity: {activity_name!r} from {from_date:%Y-%m-%d} to {to_date:%Y-%m-%d}")

    # fetch all activity lists and find the one matching the activity_name
    resp = nexus_client.get("preferences/ACTIVITY_LIST/")
    activity_lists = resp.json()

    try:
        activity_link = next(item["_links"]["self"]["href"] for item in activity_lists if item["name"] == activity_name)
    except StopIteration:
        raise ValueError(f"Could not find activity '{activity_name}' in activity lists.")

    # fetch the activity details
    resp = nexus_client.get(activity_link)
    discharge_report = resp.json()
    content_link = discharge_report["_links"]["content"]["href"]

    # fetch the content links for the activity
    resp = nexus_client.get(f"{content_link}&pageSize=50&from={from_date:%Y-%m-%dT%H:%M:%S}.000Z&to={to_date:%Y-%m-%dT%H:%M:%S}.999Z")
    content_links = resp.json()

    letters = []
    for page in content_links["pages"]:

        resp = nexus_client.get(page["_links"]["content"]["href"])
        page_content = resp.json()

        for content in page_content:
            date = pd.to_datetime(content["date"], format="%Y-%m-%dT%H:%M:%S.%f%z", errors="coerce")
            medcom_id = content["_links"]["referencedObject"]["href"].split("/")[-1]
            letters += [
                {
                    "medcom_id": medcom_id,
                    "name": content["name"],
                    "patients_id": content["patients"][0]["id"],
                    "date": date,
                    "link": content["_links"]["referencedObject"]["href"],
                }
            ]

    return letters


def get_organization_tree_info(name: str) -> dict:

    resp = nexus_client.get("organizations/tree?activeOnly=false")
    data = resp.json()
    ids = set()

    def create_sub_tree_id_list(item: list):
        for row in item:
            ids.add(row["id"])
            create_sub_tree_id_list(row["children"])

    def find_sub_tree(*, item: dict, name: str):
        if item.get("name") == name:
            create_sub_tree_id_list(item["children"])
            return item["children"]
        else:
            for child in item["children"]:
                tree = find_sub_tree(item=child, name=name)
                if tree is not None:
                    return tree

    districts = find_sub_tree(item=data, name=name)

    if mismatched_districts := ({d["name"] for d in districts} ^ {d["name"] for d in EXPECTED_DISTRICTS}):
        raise ValueError(f"Found mismatched districts: {mismatched_districts}.")

    return {"distric_tree": districts, "district_ids": ids}


def find_active_organisation(
    *,
    patient: str,
    district_ids: set,
    distric_tree: list,
) -> str:

    resp = nexus_client.get(f"patients/{patient}/organizations")
    data = resp.json()

    def find_parent_by_id(item: list, id: str) -> bool:
        found_id = False
        if item["id"] == id:
            found_id = True
            return found_id
        else:
            for row in item["children"]:
                found_id = find_parent_by_id(row, id)
                if found_id:
                    return found_id

    found_district = set()
    found_distrikt_id = []
    for org in data:
        if org["id"] in district_ids and org["effectiveAtPresent"]:
            found_distrikt_id.append(org["id"])
            for item in distric_tree:
                check_id = find_parent_by_id(item, org["id"])
                if check_id:
                    found_district.add(item["name"])
                    break

    active_districts = []

    for item in found_district:
        for district in EXPECTED_DISTRICTS:
            if item == district["name"] and district["active"]:
                active_districts.append(item)

    if len(active_districts) == 1:
        found_district = active_districts[0]
    else:
        found_district = "Ukendt"

    return found_district


def generate_report_email(letters: list[dict]) -> str:

    typography_style = "font-family: Arial, sans-serif; font-size: 12px"
    body = f"""<!DOCTYPE html>
    <html>
    <body style="margin: 0; padding: 0; {typography_style};">
    <p>Robotten har netop scannet nye breve og identificeret relevante ord i følgende dokumenter:</p>
    <table border="1" cellpadding="4" cellspacing="0" width="100%" style="border-collapse: collapse; {typography_style};">
        <tr style="background-color: #dddddd; font-weight: bold;">
        <td>Emne</td>
        <td>Dato</td>
        <td>Patient ID</td>
        <td>Link</td>
        <td>Nøgleord</td>
        </tr>"""

    # group and sort by district
    letters_by_district = defaultdict(list)
    for letter in letters:
        if letter["keywords"]:
            letters_by_district[letter["district"]] += [letter]

    for district in sorted(letters_by_district.keys()):

        body += f"""
        <tr style="background-color: #f0f0f0; font-weight: bold;">
        <td colspan="5">{district}</td>
        </tr>"""

        # sort letters within each district by patient ID first, then by date
        sorted_letters = sorted(letters_by_district[district], key=lambda x: (x["patients_id"], -x["date"].timestamp()))

        for letter in sorted_letters:
            keywords = ", ".join(letter["keywords"])
            body += f"""
            <tr>
            <td>{letter["name"]}</td>
            <td>{letter["date"].strftime("%Y-%m-%d %H:%M:%S")}</td>
            <td>{letter["patients_id"]}</td>
            <td><a href="https://nyborg.{nexus_environment}.kmd.dk/citizen/{letter["patients_id"]}/correspondence/inbox" style="color: #0000EE;">Åbn indbakke</a></td>
            <td>{keywords}</td>
            </tr>"""

    body += """
    </table>
    <p>Venlig hilsen,<br>Robotten</p>
    </body>
    </html>"""

    return body


def scan_medcom_letters():

    # get organization tree info
    org_info = get_organization_tree_info(name="Hjemmepleje")

    # fetch SharePoint lists
    print("Fetching SharePoint lists...")

    sp_keywords_list = (
        sharepoint_client.sites.get_by_url(url="https://nyborg365.sharepoint.com/sites/RPADrift")
        .lists.get_by_name(name="01.53.02 Ernæringsord liste")
        .items.get_all()
        .expand(["fields"])
        .execute_query()
    )

    sp_prev_letters_list = (
        sharepoint_client.sites.get_by_url("https://nyborg365.sharepoint.com/sites/RPADrift")
        .lists.get_by_name("01.53.01  Sundhed og Ældre - Diætist - Medcom scanner")
        .items.get_all()
        .expand(["fields"])
        .execute_query()
    )

    # fetch letters to check
    letters: list[dict] = []
    medcom_activities = ("Udskrivningsrapport", "Plejeforløbsplaner")
    for activity in medcom_activities:
        letters += fetch_medcom_letters(activity)

    # find previously processed letter ids from SharePoint
    prev_letters = {str(item.properties["fields"].properties["Title"]) for item in sp_prev_letters_list}

    # filter out previously processed letters
    letters = [letter for letter in letters if letter["medcom_id"] not in prev_letters]

    letters_to_report = []
    for letter in (pbar := tqdm(letters, desc="Processing letters")):

        # fetch letter body
        pbar.set_postfix(letter=(letter["medcom_id"], letter["name"]))
        letter_body_raw = nexus_client.get(letter["link"]).json()
        letter_body = base64.b64decode(letter_body_raw["raw"]).decode("utf-8")

        # search for keywords in letter body based on SharePoint items
        keywords = []
        for item in sp_keywords_list:
            search_word = str(item.properties["fields"].properties["Title"]).lower()
            num_matches = int(item.properties["fields"].properties["Udslag"])
            if search_word in letter_body.lower():
                keywords += [search_word]
                item.fields.set_property("Udslag", num_matches + 1)
                item.fields.update()  # needs .execute_query() to take effect

        # save to list of processed letters
        sp_prev_letters_list.add(
            fields={
                "Title": letter["medcom_id"],
                "Aktivitetsliste": letter["name"],
                "Match": bool(keywords),
                "Status": "Completed",
                "Dato": letter["date"].strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
        )

        # if letter contains keywords, add to list of matching letters
        # which will be used to generate the report email
        if keywords:
            district = find_active_organisation(patient=letter["patients_id"], district_ids=org_info["district_ids"], distric_tree=org_info["distric_tree"])
            letter |= {"keywords": keywords, "district": district}
            letters_to_report += [letter]

    # send email if there are letters to report
    if letters_to_report:
        print("Sending report email...")
        send_email(
            sender=os.environ["MS_MAILBOX"],
            # recipients=["emia@nyborg.dk", "mandr@nyborg.dk"],
            recipients=["emia@nyborg.dk", "mandr@nyborg.dk"],
            subject="Rapport: Fund af ernæringsrelaterede ord",
            body=generate_report_email(letters_to_report),
        )

    # save changes to SharePoint if we processed any letters
    if letters:
        print("Saving changes to SharePoint...")
        sharepoint_client.execute_query()


if __name__ == "__main__":

    # load environment variables
    load_dotenv(override=True)

    # initialize clients
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

    sharepoint_client = GraphClient(tenant=os.getenv("MS_GRAPH_TENANT_ID")).with_client_secret(
        client_id=os.getenv("MS_GRAPH_CLIENT_ID"),
        client_secret=os.getenv("MS_GRAPH_CLIENT_SECRET"),
    )

    # scan Medcom letters for keywords
    scan_medcom_letters()
