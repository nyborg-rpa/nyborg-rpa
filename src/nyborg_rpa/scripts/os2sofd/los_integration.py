import os
from pathlib import Path

import argh
import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm

from nyborg_rpa.utils.email import send_email
from nyborg_rpa.utils.os2sofd_client import OS2sofdClient

os2_client: OS2sofdClient


def parse_address_details(address: str) -> dict:
    """Parse an address on the format `"Street name 12, 5000 Odense C"` into a `{street, zip_code, city}` dict."""

    parts = address.split(",")
    details = {
        "street": parts[0].strip(),
        "zip_code": parts[-1].strip().split(" ", maxsplit=1)[0],
        "city": parts[-1].strip().split(" ", maxsplit=1)[1],
    }

    return details


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

    load_dotenv(override=True)
    os2_client = OS2sofdClient(kommune="nyborg")
    working_dir: Path = Path(working_dir)

    # read LOS and SD files
    los_df = (
        pd.read_excel(
            io=working_dir / "LOS.xlsx",
            dtype=str,
        )
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

    sd_df = pd.read_csv(
        filepath_or_buffer=working_dir / "AnsatteMedarbejdere.csv",
        encoding="ansi",
        sep=";",
        dtype=str,
        usecols=["CPR-nummer", "Tjenestenummer"],
    )

    # merge los and sd data on tjenestenummer
    # backfill to find afdeling from niveau 2-7 to new column "Afdeling"
    merged_df = (
        los_df.dropna(subset=["Tjenestenummer"])
        .join(sd_df.set_index("Tjenestenummer"), on="Tjenestenummer")
        .assign(Afdeling=lambda df: df[[f"Niveau {level}" for level in range(2, 8)]].bfill(axis=1).iloc[:, 0])
    )

    # build new dataframe with one row per department
    # handling special cases for certain afdeling values
    rows = []
    for row in tqdm(merged_df.iloc, total=len(merged_df), desc="Processing LOS data"):

        if pd.isna(row["Afdeling"]):
            continue

        # extract <username>@nyborg.dk based on CPR number (if available)
        username = None
        if pd.notna(row["CPR-nummer"]):
            user_info = os2_client.get_user_by_cpr(cpr=row["CPR-nummer"].replace("-", ""))
            username = next((str(user["UserId"]).lower() for user in user_info["Users"] if "@" not in user["UserId"]), None)

        match row["Afdeling"]:

            case "Tim Jeppesen":
                rows += [{"Afdeling": "Direktion", "Leder": username, "adresse": row["adresse"], "p-nummer": row["p-nummer"]}]
                rows += [{"Afdeling": "Direktionssekretariat", "Leder": username, "adresse": row["adresse"], "p-nummer": row["p-nummer"]}]

            case "Vicekommunaldirektør":
                rows += [{"Afdeling": "Sundhed og Ældre", "Leder": username, "adresse": row["adresse"], "p-nummer": row["p-nummer"]}]
                rows += [{"Afdeling": row["Afdeling"], "Leder": "anso", "adresse": "Torvet 1, 5800 Nyborg", "p-nummer": None}]

            case "Direktør":
                rows += [{"Afdeling": "Arbejdsmarked og Borgerservice", "Leder": username, "adresse": row["adresse"], "p-nummer": row["p-nummer"]}]
                rows += [{"Afdeling": row["Afdeling"], "Leder": "logl", "adresse": row["adresse"], "p-nummer": None}]

            case "Lone Grangaard Lorenzen":
                rows += [{"Afdeling": row["Niveau 5"], "Leder": username, "adresse": row["adresse"], "p-nummer": row["p-nummer"]}]

            case _:
                rows += [{"Afdeling": row["Afdeling"], "Leder": username, "adresse": row["adresse"], "p-nummer": row["p-nummer"]}]

    # create new LOS dataframe with one row per department
    # use dtype=object to keep None and represent numbers as strings
    los_df = pd.DataFrame(rows, dtype=object)

    # #️⃣ STEP 2: Merge LOS data into OS2sofd

    # fetch organizations from OS2sofd
    organizations = os2_client.get_all_organizations()

    # update each organisation with manager, address and pnr based on LOS data
    # and keep track of organizations with no match in LOS data

    orgs_without_los_match = []
    for org in tqdm(organizations, total=len(organizations), desc="Updating OS2sofd"):

        # match organization from OS2sofd with LOS data on name
        matches = los_df.query(f"Afdeling == '{org['Name']}'").drop_duplicates()
        if matches.empty:
            orgs_without_los_match += [org]
            continue

        elif len(matches) > 1:
            raise ValueError(f"Multiple matches for {org['Name']=!r}")

        # extract the only row
        row = matches.iloc[0]

        # set organization manager if present in LOS data
        if row["Leder"]:
            manager_info = os2_client.get_user_by_username(username=row["Leder"])
            os2_client.post_organization_manager(
                organization_uuid=org["Uuid"],
                user_uuid=manager_info.get("Uuid"),
            )

        else:
            tqdm.write(f"No leader found for {org['Name']}, skipping manager update.")

        # parse address and pnr from LOS data
        # and patch organization with new address and pnr
        address_details = parse_address_details(address=row["adresse"])
        pnr = row["p-nummer"] or "0"
        post_addresses = [
            {
                "master": "RPA",
                "masterId": "rpa",
                "street": address_details["street"],
                "postalCode": address_details["zip_code"],
                "city": address_details["city"],
                "localname": "",
                "country": "Danmark",
                "addressProtected": False,
                "prime": True,
            }
        ]

        if not row["p-nummer"]:
            tqdm.write(f"No p-nummer found for {org['Name']}, setting to 0.")

        did_modify = os2_client.patch_organization(
            uuid=org["Uuid"],
            json={
                "postAddresses": post_addresses,
                "pnr": pnr,
            },
        )

        if did_modify:
            tqdm.write(f"Modified {org["Name"]}")

    # #️⃣ STEP 3: Send rapport with organizations without match in LOS data

    # build dataframe with org name and full path for each org without match
    rows = []
    for org in orgs_without_los_match:
        if org["ParentUuid"]:
            org_path = os2_client.get_organization_path(org, separator=" > ")
            rows += [{"Afdeling": org["Name"], "Overliggende afdelinger": org_path}]

    df_los_mismatches = pd.DataFrame(rows).sort_values(by="Overliggende afdelinger")

    to_excel(
        df=df_los_mismatches,
        filepath=working_dir / "los_integration_error_list.xlsx",
        sheet_name="LOS Fejlliste",
    )

    # ️ build and send email with attachment
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

    # cleanup
    (working_dir / "los_integration_error_list.xlsx").unlink()


if __name__ == "__main__":

    # test parse_address_details
    for address in ["Torvet 1, 5800 Nyborg", "Nørregade 12, 5000 Odense C", "Hovedgaden 5, Mellemby, 6000 Kolding"]:
        print(f"Parsed {address=!r} into {parse_address_details(address)}")

    # los_integration(mail_recipients=["emia@nyborg.dk"], working_dir=r"C:\Users\mandr\Desktop\los-data")
    # los_integration(mail_recipients=["emia@nyborg.dk"], working_dir=r"C:\Users\emia\Downloads")
