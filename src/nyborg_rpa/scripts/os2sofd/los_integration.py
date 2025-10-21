import os
from datetime import datetime
from pathlib import Path

import argh
import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm

from nyborg_rpa.utils.email import send_email
from nyborg_rpa.utils.excel import df_to_excel_table
from nyborg_rpa.utils.os2sofd_client import OS2sofdApiClient, OS2sofdGuiClient
from nyborg_rpa.utils.pad import dispatch_pad_script

os2_api_client: OS2sofdApiClient
os2_gui_client: OS2sofdGuiClient


def parse_address_details(address: str) -> dict:
    """Parse an address on the format `"Street name 12, 5000 Odense C"` into a `{street, zip_code, city}` dict."""

    parts = address.split(",")
    details = {
        "street": parts[0].strip(),
        "zip_code": parts[-1].strip().split(" ", maxsplit=1)[0],
        "city": parts[-1].strip().split(" ", maxsplit=1)[1],
    }

    return details


@argh.arg("--mail_recipients", help="List of email recipients for the report.", nargs="*")
@argh.arg("--working_dir", help="Path containg data (LOS excel) and output.")
def los_integration(*, mail_recipients: list[str], working_dir: Path | str):
    """Merge LOS data into OS2sofd and send rapport with mismatch."""

    global os2_api_client
    global os2_gui_client

    load_dotenv(override=True)
    os2_api_client = OS2sofdApiClient(kommune="nyborg")
    os2_gui_client = OS2sofdGuiClient(user="Roboit", kommune="nyborg")
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
            user_info = os2_api_client.get_user_by_cpr(cpr=row["CPR-nummer"].replace("-", ""))
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
    organizations = os2_api_client.get_all_organizations()

    # update each organisation with manager, address and pnr based on LOS data
    # and keep track of organizations with no match in LOS data

    orgs_without_los_match = []
    for org in tqdm(organizations, total=len(organizations), desc="Updating OS2sofd"):

        # an "rpa-override" tag without value will skip the organization or
        # use value to match on instead of the actual name
        has_override_tag = any(tag["Tag"] == "rpa-override" for tag in org.get("Tags", []))
        override_value = next((tag["CustomValue"] for tag in org.get("Tags", []) if tag["Tag"] == "rpa-override"), None)

        if has_override_tag and not override_value:
            tqdm.write(f"Skipping {org["Name"]!r} due to rpa-override tag without value.")
            continue

        if override_value:
            tqdm.write(f"Using override {org["Name"]!r} → {override_value!r}.")

        tqdm.write(f"Processing {org["Name"]!r}...")

        # add Source field to indicate where the name came from
        org_name = override_value or org["Name"]
        org["Source"] = "RPA Override" if override_value else "LOS"

        # match organization from OS2sofd with LOS data on name
        matches = los_df.query(f"Afdeling == '{org_name}'").drop_duplicates()
        if matches.empty:
            orgs_without_los_match += [org]
            continue

        elif len(matches) > 1:
            raise ValueError(f"Multiple matches for {org_name!r}")

        # extract the only row
        row = matches.iloc[0]

        # set organization manager if present in LOS data
        if manager_username := row["Leder"]:
            manager_info = os2_api_client.get_user_by_username(manager_username)
            tqdm.write(f"Updating {org_name!r} with manager={manager_username!r}.")
            os2_api_client.post_organization_manager(
                organization_uuid=org["Uuid"],
                user_uuid=manager_info.get("Uuid"),
            )

        # parse address and pnr from LOS data
        # edit organization with new pnr
        org_coreinfo = os2_gui_client.get_organization_coreinfo(uuid=org["Uuid"])
        pnr: str | None = row["p-nummer"] or None
        if pnr is not None and not pnr.isdigit():
            orgs_without_los_match += [org]
            continue

        org_coreinfo["pnr"] = pnr
        tqdm.write(f"Updating {org_name!r} with {pnr=!r}...")
        os2_gui_client.post_organization_coreinfo(uuid=org["Uuid"], data=org_coreinfo)

        # edit organization with new address
        if pd.isna(row["adresse"]):
            orgs_without_los_match += [org]
            continue

        # if primary address exists, update it; otherwise create new primary address
        address_details = parse_address_details(address=row["adresse"])
        org_addresses = os2_gui_client.get_organization_addresses(uuid=org["Uuid"])
        primary_address = next((address for address in org_addresses if address["prime"]), None)

        if primary_address is None:
            primary_address = {
                "id": "",
                "street": address_details["street"],
                "postalCode": address_details["zip_code"],
                "city": address_details["city"],
                "localname": "",
                "country": "Danmark",
                "returnAddress": True,
                "prime": True,
            }

        else:
            primary_address["street"] = address_details["street"]
            primary_address["postalCode"] = address_details["zip_code"]
            primary_address["city"] = address_details["city"]

        tqdm.write(f"Updating {org_name!r} with address={address_details!r}...")
        os2_gui_client.edit_or_create_organization_address(uuid=org["Uuid"], address=primary_address)

    # #️⃣ STEP 3: Send report with organizations without match in LOS data if monday
    if datetime.today().strftime("%A") != "Monday":
        return

    # build dataframe with org name and full path for each org without match
    rows = []
    for org in orgs_without_los_match:
        if org["ParentUuid"]:
            org_path = os2_api_client.get_organization_path(org, separator=" > ")
            rows += [{"Afdeling": org["Name"], "Kilde": org["Source"], "Overliggende afdelinger": org_path}]

    df_los_mismatches = pd.DataFrame(rows).sort_values(by="Overliggende afdelinger")

    df_to_excel_table(
        df=df_los_mismatches,
        filepath=working_dir / "los_integration_error_list.xlsx",
        sheet_name="LOS Fejlliste",
    )

    typography_style = "font-family: Arial, sans-serif; font-size: 12px"
    body = f"""<!DOCTYPE html>
    <html>
    <body style="margin:0; padding:0; {typography_style}; line-height:1.4;">
    <p>Vedhæftet finder du <strong>los_integration_error_list.xlsx</strong> med afdelinger, som ikke kunne matches i LOS.</p>
    <p>Venlig hilsen,<br>Robotten</p>
    </body>
    </html>"""

    send_email(
        sender=os.environ["MS_MAILBOX"],
        recipients=mail_recipients,
        subject="Rapport: LOS integration OS2sofd - Fejlliste",
        body=body,
        attachments=[working_dir / "los_integration_error_list.xlsx"],
    )

    # cleanup
    (working_dir / "los_integration_error_list.xlsx").unlink()


if __name__ == "__main__":
    dispatch_pad_script(fn=los_integration)
    # los_integration(mail_recipients=[], working_dir=r"C:\Users\\Downloads")
