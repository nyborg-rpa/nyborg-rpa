import json
import locale
import os
from datetime import date, datetime
from pathlib import Path
from typing import TypedDict

import pandas as pd
from dotenv import load_dotenv
from xlsxwriter.utility import xl_range
from xlsxwriter.worksheet import Worksheet

from nyborg_rpa.utils.datafordeler import DatafordelerClient, parse_address
from nyborg_rpa.utils.email import send_email
from nyborg_rpa.utils.excel import df_to_excel_table

client: DatafordelerClient


class Resident(TypedDict):
    """A simplified representation of a Person from Datafordeler CPR."""

    cpr: str
    name: str
    address: str
    birthday: date
    civil_status: str
    civil_valid_from: date
    partner_cpr: str | None

    @classmethod
    def from_datafordeler_person(cls, person: dict) -> "Resident":

        # check that data doesn't contain "status: "historisk"
        if '"status": "historisk"' in json.dumps(person, ensure_ascii=False):
            raise ValueError(f"Person object with id={person['id']!r} contains historical records.")

        # basic info
        resident = {
            "id": person["id"],
            "birthday": datetime.fromisoformat(person["foedselsdato"]).date(),
            "gender": person["koen"],
            "status": person["status"],
        }

        # extended info
        resident["name"] = person["Navne"][0]["Navn"]["adresseringsnavn"].replace(",", ", ")
        resident["cpr"] = person["Personnumre"][0]["Personnummer"]["personnummer"]

        # address
        address_dict = person["Adresseoplysninger"][0]["Adresseoplysninger"]["CprAdresse"]
        resident["address"] = parse_address(address_dict)

        # civil info and partner
        civil_info = person["Civilstande"][0]["Civilstand"]
        resident["civil_status"] = civil_info["Civilstandstype"]
        resident["civil_valid_from"] = datetime.fromisoformat(civil_info["virkningFra"]).date()
        resident["partner_cpr"] = civil_info.get("Aegtefaelle", {}).get("aegtefaellePersonnummer")

        return cls(**resident)


def find_residents_turning_age_for_year(*, age: int, year: int) -> pd.DataFrame:
    """Find residents turning a specific age in a given year."""

    persons = client.get_persons(
        params={
            "person.foedselsdato.ge": f"{year - age}-01-01",
            "person.foedselsdato.le": f"{year - age}-12-31",
            "cadr.cprkommunekode.eq": "450",
            "adropl.fraflytningskommunekode.ne": "450",
            "person.status.wi": "bopael_i_danmark|bopael_i_danmark_hoej_vejkode",
        },
    )

    residents = [Resident.from_datafordeler_person(p) for p in persons]
    cols = list(Resident.__annotations__.keys())
    df = pd.DataFrame(data=residents, columns=cols)

    return df


def find_residents_with_wedding_anniversaries_for_year(*, anniversaries: list[int], year: int) -> pd.DataFrame:
    """Find residents with wedding anniversaries in a given year."""

    residents = []
    for anniversary in anniversaries:

        persons = client.get_persons(
            params={
                "civ.status.eq": "aktuel",
                "civ.civilstandstype.eq": "gift",
                "civ.virkningfra.ge": f"{year - anniversary}-01-01",
                "civ.virkningfra.le": f"{year - anniversary}-12-31",
                "cadr.cprkommunekode.eq": "450",
                "adropl.fraflytningskommunekode.ne": "450",
                "person.status.wi": "bopael_i_danmark|bopael_i_danmark_hoej_vejkode",
            },
        )

        for p in persons:
            r = Resident.from_datafordeler_person(p)
            r["anniversary"] = anniversary
            r["couple_id"] = "".join(sorted([r["cpr"], r["partner_cpr"]]))
            residents += [r]

    cols = ["anniversary", "couple_id"] + list(Resident.__annotations__.keys())
    df = pd.DataFrame(data=residents, columns=cols)

    return df


def anniversaries_df_to_excel_table(*, df: pd.DataFrame, filepath: Path | str, sheet_name: str = "Sheet1") -> None:
    """Write anniversaries DataFrame to an Excel file as a formatted table."""

    filepath = Path(filepath)
    writer = pd.ExcelWriter(filepath, engine="xlsxwriter")

    # write Excel file
    df.to_excel(writer, sheet_name=sheet_name, startrow=1, header=False, index=False)

    # format Excel sheet
    ws: Worksheet = writer.sheets[sheet_name]
    rows, cols = df.shape
    ws.add_table(0, 0, rows, cols - 1, {"columns": [{"header": c} for c in df.columns], "style": "Table Style Medium 2", "banded_rows": False})
    ws.set_column(0, cols - 1, 1)
    ws.autofit()
    ws.set_column("A:A", None, None, {"hidden": True})

    # silence "Number stored as text" over the data range
    ws.ignore_errors({"number_stored_as_text": xl_range(1, 0, rows, cols - 1)})

    # apply alternating row colors, changing color when "couple_id" changes
    current_fmt = 0
    fmts = [writer.book.add_format({"bg_color": c}) for c in ["#FFFFFF", "#DCE6F1"]]

    for i in range(rows):

        if i > 0 and df["couple_id"].iat[i] != df["couple_id"].iat[i - 1]:
            current_fmt ^= 1  # flip color when id changes

        fmt = fmts[current_fmt]
        for c in range(cols):
            val = df.iat[i, c]
            ws.write(i + 1, c, val, fmt)

    writer.close()


def resident_milestones_for_next_year(
    *,
    working_dir: Path | str,
    mail_recipients: list[str],
):
    """
    Find residents with upcoming 100-years birthdays and wedding anniversaries next year,
    generate Excel files, and send them via email.
    """

    load_dotenv(override=True)

    # initialize Datafordeler client
    global client
    client = DatafordelerClient()

    # determine the year to look for milestones in
    today = datetime.today()
    year = today.year + 1

    # set Danish locale for date formatting
    locale.setlocale(locale.LC_ALL, "da_DK.UTF-8")

    # find residents with 100-years birthday next year
    df_hundred_years = (
        find_residents_turning_age_for_year(age=100, year=year)
        .sort_values(by="birthday")
        .assign(birthday=lambda df: pd.to_datetime(df["birthday"]).dt.strftime(f"%d. %B {year}"))  # format as "1. januar {year}"
        .rename(columns=(c := {"name": "Navn", "address": "Adresse", "birthday": "Fødselsdag", "cpr": "Personnummer"}))
        .filter(items=c.values())
        .reset_index(drop=True)
    )

    # find residents with wedding anniversary next year
    df_anniversaries = (
        find_residents_with_wedding_anniversaries_for_year(
            anniversaries=[60, 65, 70, 75, 80],
            year=year,
        )
        .sort_values(by=["anniversary", "couple_id", "cpr"])
        .assign(civil_valid_from=lambda df: pd.to_datetime(df["civil_valid_from"]).dt.strftime("%d. %B %Y"))
        .astype(str)
        .assign(anniversary=lambda df: df["anniversary"] + " år")
        .rename(
            columns=(
                c := {
                    "couple_id": "couple_id",
                    "anniversary": "Jubilæum",
                    "civil_valid_from": "Vielsesdato",
                    "name": "Navn",
                    "address": "Adresse",
                    "cpr": "Personnummer",
                }
            )
        )
        .filter(items=c.values())
        # group by id and blank out duplicate Jubilæum/Vielsesdato within each couple_id
        .pipe(lambda d: d.assign(**{col: d[col].mask(d.duplicated("couple_id"), "") for col in ("Jubilæum", "Vielsesdato")}))
    )

    # generate Excel files
    working_dir = Path(working_dir)
    residents_hundred_years_file = working_dir / "residents_hundred_years.xlsx"
    residents_wedding_anniversary_file = working_dir / "residents_wedding_anniversary.xlsx"

    df_to_excel_table(df=df_hundred_years, filepath=residents_hundred_years_file)
    anniversaries_df_to_excel_table(df=df_anniversaries, filepath=residents_wedding_anniversary_file)

    # send email with Excel files attached
    typography_style = "font-family: Arial, sans-serif; font-size: 12px"
    body = f"""<!DOCTYPE html>
    <html>
    <body style="margin:0; padding:0; {typography_style}; line-height:1.4;">
    <p>Vedhæftet finder du <strong>residents_hundred_years.xlsx</strong> med kommende 100 års fødselsdage og <strong>residents_wedding_anniversary.xlsx</strong> med bryllupsjubilæer.</p>
    <p>Venlig hilsen,<br>Robotten</p>
    </body>
    </html>"""

    send_email(
        sender=os.environ["MS_MAILBOX"],
        recipients=mail_recipients,
        subject=f"Fødselsdage og bryllupsjubilæer i {year}",
        body=body,
        attachments=[residents_hundred_years_file, residents_wedding_anniversary_file],
    )

    # cleanup
    residents_wedding_anniversary_file.unlink()
    residents_hundred_years_file.unlink()


if __name__ == "__main__":
    user = os.getlogin()
    resident_milestones_for_next_year(working_dir=f"C:/Users/{user}/Downloads", mail_recipients=[f"{user}@nyborg.dk"])
