import base64
import os
from collections import defaultdict

import pandas as pd
from dotenv import load_dotenv
from office365.graph_client import GraphClient
from tqdm.auto import tqdm

from nyborg_rpa.utils.nexus_client import NexusClient
from nyborg_rpa.utils.send_mail import send_email

EXPECTED_DISTRICT = [
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
]

# nexus_client = NexusClient(enviroment="nexus-review")
enviroment = "nexus"
nexus_client = NexusClient(enviroment=enviroment)


def get_medcom_letters(activity_liste: str) -> list:
    resp = nexus_client.get("preferences/ACTIVITY_LIST/")
    activity_list = resp.json()

    for item in activity_list:
        if item["name"] == activity_liste:
            activity_link = item["_links"]["self"]["href"]
            break

    resp = nexus_client.get(activity_link)
    discharge_report = resp.json()

    content_link = discharge_report["_links"]["content"]["href"]

    # to_date = pd.Timestamp.now() - pd.Timedelta(days=180)
    to_date = pd.Timestamp.now()
    to_date_str = to_date.strftime("%Y-%m-%dT%H:%M:%S.999Z")
    from_date = to_date - pd.Timedelta(days=7)
    from_date_str = from_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    resp = nexus_client.get(f"{content_link}&pageSize=50&from={from_date_str}&to={to_date_str}")
    content_links = resp.json()

    letter_list = []

    # print("Loading pages")
    # for page in tqdm(content_links["pages"]):
    for page in content_links["pages"]:
        page_content = nexus_client.get(page["_links"]["content"]["href"]).json()
        for content in page_content:
            # date = datetime.strptime(content["date"], '%Y-%m-%dT%H:%M:%S.%f%z')
            date = pd.to_datetime(content["date"], format="%Y-%m-%dT%H:%M:%S.%f%z", errors="coerce")
            medcom_id = content["_links"]["referencedObject"]["href"].split("/")[-1]
            letter = {
                "medcom_id": medcom_id,
                "name": content["name"],
                "patients_id": content["patients"][0]["id"],
                "date": date,
                "link": content["_links"]["referencedObject"]["href"],
            }
            letter_list.append(letter)
    return letter_list


def get_ogranisation_tree_info(organisation: str) -> dict:
    resp = nexus_client.get("organizations/tree?activeOnly=false")
    data = resp.json()
    ids = []

    def create_sub_tree_id_list(item: list):
        for row in item:
            ids.append(row["id"])
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

    result = {
        "distric_tree": find_sub_tree(item=data, name=organisation),
        "distric_ids": set(ids),
    }
    return result


def find_active_organisation(patient: str, distric_ids: set, distric_tree: list) -> str:
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

    found_distrikt = set()
    found_distrikt_id = []
    for org in data:
        if org["id"] in distric_ids and org["effectiveAtPresent"]:
            found_distrikt_id.append(org["id"])
            for item in distric_tree:
                check_id = find_parent_by_id(item, org["id"])
                if check_id:
                    found_distrikt.add(item["name"])
                    break

    active_distrik = []

    for item in found_distrikt:
        for district in EXPECTED_DISTRICT:
            if item == district["name"] and district["active"]:
                active_distrik.append(item)

    if len(active_distrik) == 1:
        found_distrikt = active_distrik[0]
    else:
        found_distrikt = "Ukendt"

    return found_distrikt


def create_mail_message_2(mails: dict) -> str:
    brødtekst = """<!DOCTYPE html>
    <html>
    <body style="font-family: Arial, sans-serif; font-size: 14px; margin: 0; padding: 0;">
    <h2 style="font-size: 16px; font-weight: bold;">Rapport: Fund af ernæringsrelaterede ord</h2>
    <p>Hej,</p>
    <p>Robotten har netop scannet nye breve og identificeret relevante ord i følgende dokumenter:</p>
    <table border="1" cellpadding="4" cellspacing="0" width="100%" style="border-collapse: collapse; font-size: 14px;">
        <tr style="background-color: #dddddd; font-weight: bold;">
        <td>Emne</td>
        <td>Dato</td>
        <td>Patient ID</td>
        <td>Link</td>
        <td>Nøgleord</td>
        </tr>"""

    # Group and sort by district
    grouped = defaultdict(list)
    for mail in mails:
        if mail["found_words"]:
            grouped[mail["district"]] += [mail]

    for district in sorted(grouped.keys()):
        brødtekst += f"""
        <tr style="background-color: #f0f0f0; font-weight: bold;">
        <td colspan="5">{district}</td>
        </tr>"""
        for mail in grouped[district]:
            nøgleord = ", ".join(mail["found_words"])
            brødtekst += f"""
        <tr>
        <td>{mail['letter']['name']}</td>
        <td>{mail['letter']['date'].strftime("%Y-%m-%d %H:%M:%S")}</td>
        <td>{mail['letter']['patients_id']}</td>
        <td><a href="https://nyborg.{enviroment}.kmd.dk/citizen/{mail['letter']['patients_id']}/correspondence/inbox" style="color: #0000EE;">Åbn indbakke</a></td>
        <td>{nøgleord}</td>
        </tr>"""

    brødtekst += """
    </table>
    <p>Venlig hilsen,

    Robotten</p>
    </body>
    </html>"""
    return brødtekst


def create_mail_message_1(mails: dict) -> str:
    brødtekst = """<!DOCTYPE html>
    <html>
    <head>
    <meta charset="UTF-8">
    <style>
        body { font-family: Arial, sans-serif; }
        .container { max-width: 600px; margin: 0 auto; padding: 1em; background-color: #ffffff; border-radius: 8px; }
        .document { margin-bottom: 20px; padding: 1em; background-color: #fff; border: 1px solid #ddd; border-radius: 6px; }
        .document h3 { margin-top: 0; }
        .found-words { margin-top: 8px; padding-left: 20px; }
    </style>
    </head>
    <body>
    <div class="container">
        <h2>Resultat af den daglige scanning: Fund af Ernæringsord</h2>
        <p>Hej,</p>
        <p>Robotten har netop scannet nye breve og fundet følgende relevante ord i nedenstående dokumenter:</p>"""

    for mail in mails:
        if mail["found_words"] == []:
            continue
        tekst = f"""
        <div class="document">
        <p><strong>Distrikt: </strong> {mail['district']}<br>
            <strong>Emne: </strong> {mail['letter']['name']}<br>
            <strong>Dato:</strong> {mail['letter']['date'].strftime("%Y-%m-%d %H:%M:%S")}<br>
            <strong>Patients ID:</strong> {mail['letter']['patients_id']}<br>
            <strong>Link til patient indbakke:</strong> <a href="https://nyborg.{enviroment}.kmd.dk/citizen/{mail['letter']['patients_id']}/correspondence/inbox">Åbn indbakke</a></p>
        <p><strong>Fundne ord:</strong> {mail['found_words']}</p>
        </div>
        """

        brødtekst += tekst

    brødtekst += """<p>Venlig hilsen,<br>
        Robotten</p>
    </div>
    </body>
    </html>"""

    return brødtekst


if __name__ == "__main__":
    load_dotenv(override=True)

    sharepoint_client = GraphClient(tenant=os.getenv("MS_GRAPH_TENANT_ID")).with_client_secret(
        client_id=os.getenv("MS_GRAPH_CLIENT_ID"),
        client_secret=os.getenv("MS_GRAPH_CLIENT_SECRET"),
    )

    items = sharepoint_client.sites.get_by_url("https://nyborg365.sharepoint.com/sites/RPADrift").lists.get_by_name("01.53.02 Ernæringsord liste").items.get_all().expand(["fields"]).execute_query()

    sp_list = (
        sharepoint_client.sites.get_by_url("https://nyborg365.sharepoint.com/sites/RPADrift")
        .lists.get_by_name("01.53.01  Sundhed og Ældre - Diætist - Medcom scanner")
        .items.get_all()
        .expand(["fields"])
        .execute_query()
    )

    def check_letters(letter_list: dict):
        for letter in tqdm(letter_list):
            found_words = []
            link = letter["link"]
            if letter["medcom_id"] in former_runs_item:
                continue
            medcom = nexus_client.get(link).json()
            decoded_xml = base64.b64decode(medcom["raw"]).decode("utf-8")
            for i, item in enumerate(items):
                search_word = item.properties["fields"].properties["Title"]
                current_udslag = item.properties["fields"].properties["Udslag"]
                if search_word.lower() in decoded_xml.lower():
                    found_words.append(search_word.lower())
                    item.fields.set_property("Udslag", current_udslag + 1)
                    item.fields.update()
            # if found_words != []:
            mail = {
                "letter": letter,
                "found_words": found_words,
                "district": find_active_organisation(patient=letter["patients_id"], distric_ids=organiastion["distric_ids"], distric_tree=organiastion["distric_tree"]),
            }
            mails.append(mail)
            # Testing
            fields = {
                "Title": mail["letter"]["medcom_id"],
                "Aktivitetsliste": mail["letter"]["name"],
                "Match": True if mail["found_words"] != [] else False,
                "Status": "Completed",
                "Dato": mail["letter"]["date"].strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
            sp_list.add(fields=fields)

    Udskrivningsrapport = get_medcom_letters("Udskrivningsrapport")
    Plejeforløbsplaner = get_medcom_letters("Plejeforløbsplaner")
    organiastion = get_ogranisation_tree_info("Hjemmepleje")
    # active_organisation = set(find_active_organisation(patient=1, distric_ids=organiastion["distric_ids"], distric_tree=organiastion["distric_tree"]))

    main_district = {district["name"] for district in organiastion["distric_tree"]}
    expected_dist = {district["name"] for district in EXPECTED_DISTRICT}
    if expected_dist != main_district:
        raise ValueError("Distrikterne har ændret sig")

    mails = []
    former_runs_item = [item.properties["fields"].properties["Title"] for item in sp_list if item.properties["fields"].properties["Aktivitetsliste"] == "Udskrivningsrapport"]
    print("checking letters in Udskrivningsrapport")
    check_letters(Udskrivningsrapport)
    former_runs_item = [item.properties["fields"].properties["Title"] for item in sp_list if item.properties["fields"].properties["Aktivitetsliste"] == "Plejeforløbsplan"]
    print("checking letters in Plejeforløbsplaner")
    check_letters(Plejeforløbsplaner)

    # sharepoint_client.execute_batch()
    print("Sendt brev 1...")
    brødtekst = create_mail_message_1(mails)
    send_email(to_addr="<email>@nyborg.dk", subject="Brev1", html_body=brødtekst)

    print("Sendt brev 2...")
    brødtekst = create_mail_message_2(mails)
    send_email(to_addr="<email>@nyborg.dk", subject="Brev2", html_body=brødtekst)

    print("loading to sharepoint...")
    sharepoint_client.execute_query()
