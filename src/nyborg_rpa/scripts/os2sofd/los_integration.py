import os
from pathlib import Path

import argh
import pandas as pd
from dotenv import load_dotenv

from nyborg_rpa.utils.email import send_email
from nyborg_rpa.utils.os2sofd_client import OS2sofdClient

os2_client: OS2sofdClient


def get_street_info(*, street: str) -> dict:
    """Will split street string into dict"""
    # Split by ,
    parts = street.split(",")
    street = parts[0].strip()

    # Split by space
    zip_city = parts[len(parts) - 1].strip().split(" ", 1)
    zip_code = zip_city[0]
    city = zip_city[1]

    output = {"street": street, "zip_code": zip_code, "city": city}

    return output


def get_organisation_parrent_path(organisation: dict) -> str:
    """Will get organisation parrent path"""
    if organisation["ParentUuid"]:
        parrent_orgination = os2_client.get_organization_by_uuid(uuid=organisation["ParentUuid"])
        parrent_name = parrent_orgination["Name"]
        next_parrent = get_organisation_parrent_path(parrent_orgination)
        if next_parrent == parrent_name:
            path = f"{parrent_name}"
        else:
            path = f"{next_parrent} > {parrent_name}"
    else:
        path = organisation["Name"]

    return path


def to_excel(*, df: pd.DataFrame, filepath: str, sheet_name: str):
    """Saves DataFrame as native Excel table autofitting columns"""

    with pd.ExcelWriter(filepath, engine="xlsxwriter") as writer:

        df.to_excel(
            excel_writer=writer,
            sheet_name=sheet_name,
            startrow=1,
            header=False,
            index=False,
        )

        rows, cols = df.shape
        worksheet = writer.sheets[sheet_name]
        column_settings = [{"header": column} for column in df.columns]

        worksheet.add_table(0, 0, rows, cols - 1, {"columns": column_settings, "style": "Table Style Medium 2"})
        worksheet.set_column(0, cols - 1, 1)
        worksheet.autofit()


# @argh.arg(help="Merge LOS data into OS2sofd and send rapport with mismatch.", nargs="*")
def los_integration(*, mail_recipients: list[str], working_dir: str):
    """Merge LOS data into OS2sofd and send rapport with mismatch."""
    global os2_client
    working_dir = Path(working_dir)

    # load environment variables
    load_dotenv(override=True)

    # Creating os2soft client connection
    os2_client = OS2sofdClient(kommune="nyborg")

    # Getting list of all organisation in OS2sofd
    organisations = os2_client.get_all_organizations()

    # Getting LOS data from LOS file and SD file
    los_file = Path(os.getenv("LOS_FILE"))
    sd_file = Path(os.getenv("SD_FILE"))

    los_df = (
        pd.read_excel(los_file, dtype=str)
        .rename(
            columns={
                "Tjeneste nr.": "Tjenestenummer",
                "Niveau 9": "p-nummer",
                "Niveau 10": "adresse",
            }
        )
        .apply(lambda x: x.str.strip() if x.dtype == object else x)
        .replace({"": pd.NA})
    )
    sd_df = pd.read_csv(sd_file, encoding="ansi", sep=";", dtype=str)[["CPR-nummer", "Tjenestenummer"]]

    merged_df = los_df.dropna(subset=["Tjenestenummer"]).join(sd_df.set_index("Tjenestenummer"), on="Tjenestenummer")
    merged_df["Afdeling"] = merged_df[[f"Niveau {level}" for level in range(2, 8)]].bfill(axis=1).iloc[:, 0]

    rows = []
    for item in merged_df.iloc:
        if pd.isna(item["Afdeling"]):
            continue

        if pd.notna(item["CPR-nummer"]):
            user_info = os2_client.get_user_by_cpr(cpr=item["CPR-nummer"].replace("-", ""))
            user_name = next((user["UserId"] for user in user_info["Users"] if "@" not in user["UserId"]), None)
            if user_name is None:
                user_name = None
            else:
                user_name = user_name.lower()
        else:
            user_name = None

        match item["Afdeling"]:
            case "Tim Jeppesen":
                rows += [{"Afdeling": "Direktion", "Leder": user_name, "adresse": item["adresse"], "p-nummer": item["p-nummer"]}]
                rows += [{"Afdeling": "Direktionssekretariat", "Leder": user_name, "adresse": item["adresse"], "p-nummer": item["p-nummer"]}]

            case "Vicekommunaldirektør":
                rows += [{"Afdeling": "Sundhed og Ældre", "Leder": user_name, "adresse": item["adresse"], "p-nummer": item["p-nummer"]}]
                rows += [{"Afdeling": item["Afdeling"], "Leder": "anso", "adresse": "Torvet 1, 5800 Nyborg", "p-nummer": None}]

            case "Direktør":
                rows += [{"Afdeling": "Arbejdsmarked og Borgerservice", "Leder": user_name, "adresse": item["adresse"], "p-nummer": item["p-nummer"]}]
                rows += [{"Afdeling": item["Afdeling"], "Leder": "logl", "adresse": item["adresse"], "p-nummer": None}]

            case "Lone Grangaard Lorenzen":
                rows += [{"Afdeling": item["Niveau 5"], "Leder": user_name, "adresse": item["adresse"], "p-nummer": item["p-nummer"]}]

            case _:
                rows += [{"Afdeling": item["Afdeling"], "Leder": user_name, "adresse": item["adresse"], "p-nummer": item["p-nummer"]}]

    los_df = pd.DataFrame(rows).astype(str)

    # #️ starting to comparing and modifi los into os2sofd

    no_match_organisations = []

    for organisation in organisations:
        match = los_df[los_df["Afdeling"] == organisation["Name"]]
        if match.empty:
            no_match_organisations.append(organisation)
            continue
        else:
            # #️ STEP 1
            # Modifi manager to organisation
            if match["Leder"].values[0] != "None":
                leader_info = os2_client.get_user_by_username(match["Leder"].values[0])
                leader_uuid = leader_info.get("Uuid")
                try:
                    os2_client.post_organization_manager(organization_uuid=organisation["Uuid"], user_uuid=leader_uuid)
                except Exception as e:
                    print(f"Failed to set manager: {e}")

            # #️ STEP 2
            # Modifi address and pnr to organisation
            address_info = get_street_info(street=match["adresse"].values[0])
            post_addresses = [
                {
                    "master": "SOFD",
                    "masterId": f"a{organisation["Uuid"]}",
                    "street": address_info["street"],
                    "postalCode": address_info["zip_code"],
                    "city": address_info["city"],
                    "localname": "",
                    "country": "Danmark",
                    "addressProtected": False,
                    "prime": True,
                }
            ]
            json = {}
            json["postAddresses"] = post_addresses
            # TODO: skal laves smartere
            if match["p-nummer"].values[0] != "None":
                json["pnr"] = match["p-nummer"].values[0]
            json["pnr"] = match["p-nummer"].values[0] if match["p-nummer"].values[0] != "None" else "0"

            try:
                status = os2_client.patch_organization(uuid=organisation["Uuid"], json=json)
                if status:
                    print(f"Modified {organisation["Name"]}")
            except Exception as e:
                print(f"Failed to set address and pnr: {e}")

    # #️ generate list of merge error
    rows = []
    for organisation in no_match_organisations:
        if organisation["ParentUuid"]:
            parrent_path = get_organisation_parrent_path(organisation=organisation)
            rows += [{"Afdeling": organisation["Name"], "Overliggende afdelinger": parrent_path}]

    rows_sorted = sorted(rows, key=lambda x: x["Overliggende afdelinger"])

    no_match_df = pd.DataFrame(rows_sorted)

    to_excel(
        df=no_match_df,
        filepath=working_dir / "los_integration_error_list.xlsx",
        sheet_name="los_integration_error",
    )

    # #️ Send an email rapport and delete file
    html_body = """
        <div style="font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; line-height:1.45;">
        <h2 style="margin:0 0 8px;">Rapport: LOS integration OS2sofd - Fejlliste</h2>
        <p>Hej,</p>
        <p>
            Vedhæftet finder du <strong>los_integration_error_list.xlsx</strong> med afdelinger,
            som ikke kunne matches.
        </p>

        <h3 style="margin:16px 0 6px;">Hvad indeholder rapporten?</h3>
        <ul style="margin:0 0 12px 20px;">
            <li>Kolonnen <em>Afdeling</em> - navnet på afdelingen i OS2sofd, der ikke fandt et match i LOS.</li>
            <li>Kolonnen <em>Overliggende afdelinger</em> - hierarkisk sti for at lette den manuelle lokalisering.</li>
        </ul>

        <h3 style="margin:16px 0 6px;">Hvad kan man gøre?</h3>
        <ul style="margin:0 0 12px 20px;">
            <li>Ret afdelingens navn i SD eller LOS arket, således afdeligen matcher.</li>
            <li>Kontakt RPA teamet for at tilføje afdeling til blacklisten, således integration undtager afdelingen <em>Bruges hvis det er en afdeling, som er udenfor LOS arket</em></li>
        </ul>

        <p style="margin:12px 0 0;">
            Kontakt gerne RPA/Integrationsteamet hvis noget ser forkert ud.
        </p>

        <hr style="border:none;border-top:1px solid #ddd;margin:16px 0;">
        <p style="font-size:12px;color:#666;margin:0;">
            Denne mail er genereret automatisk af LOS-integrationens natlige kørsel.
        </p>
        </div>
        """

    send_email(
        sender=os.getenv("MS_MAILBOX"),
        recipients=mail_recipients,
        subject="Rapport: LOS integration OS2sofd - Fejlliste",
        body=html_body,
        attachments=[working_dir / "los_integration_error_list.xlsx"],
    )

    os.remove(working_dir / "los_integration_error_list.xlsx")
    print(f"{working_dir / "los_integration_error_list.xlsx"} deleted successfully.")


if __name__ == "__main__":
    los_integration(mail_recipients=["emia@nyborg.dk"], working_dir=r"C:\Users\emia\Downloads")
