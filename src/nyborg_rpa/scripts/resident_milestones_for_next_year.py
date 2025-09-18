import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from xlsxwriter.utility import xl_range
from xlsxwriter.worksheet import Worksheet

from nyborg_rpa.utils.datafordeler import DatafordelerClient
from nyborg_rpa.utils.email import send_email
from nyborg_rpa.utils.excel import df_to_excel_table

client: DatafordelerClient


def find_residents_turning_age_for_year(*, age: int, year: int) -> pd.DataFrame:
    """Find residents turning a specific age in a given year."""

    resp = client.get(
        url="https://s5-certservices.datafordeler.dk/CPR/CPRPersonFullComplete/1/REST/PersonFullCurrentListComplete",
        params={
            "person.foedselsdato.ge": f"{year - age}-01-01",
            "person.foedselsdato.le": f"{year - age}-12-31",
            "cadr.cprkommunekode.eq": "450",
            "adropl.fraflytningskommunekode.ne": "450",
            "person.status.wi": "bopael_i_danmark|bopael_i_danmark_hoej_vejkode",
        },
    )

    resp.raise_for_status()
    data = resp.json()
    residents = client.fech_citizens_data(data=data)

    df = (
        pd.DataFrame(
            data=residents,
            columns=["name", "address", "birthday", "cpr"],
            dtype=str,
        )
        .sort_values(by="birthday")
        .reset_index(drop=True)
    )

    return df


def find_residents_with_wedding_anniversaries_for_year(*, anniversaries: list[int], year: int) -> pd.DataFrame:
    """Find residents with wedding anniversaries in a given year."""

    all_residents = []
    for anniversary in anniversaries:

        resp = client.get(
            url="https://s5-certservices.datafordeler.dk/CPR/CPRPersonFullComplete/1/REST/PersonFullCurrentListComplete",
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

        resp.raise_for_status()
        data = resp.json()
        residents = client.fech_citizens_data(data=data)

        for r in residents:
            r["anniversary"] = anniversary
            r["couple_id"] = "".join(sorted([r["cpr"], r["partner_cpr"]]))
            all_residents += [r]

    df = (
        pd.DataFrame(
            data=all_residents,
            columns=["couple_id", "anniversary", "civil_valid_from", "name", "address", "cpr", "partner_cpr"],
            dtype=str,
        )
        .sort_values(by=["anniversary", "couple_id", "cpr"])
        .reset_index(drop=True)
    )

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

    # find residents with 100-years birthday next year
    df_hundred_years = (
        find_residents_turning_age_for_year(age=100, year=year)
        .assign(birthday=lambda df: pd.to_datetime(df["birthday"]).dt.strftime(f"{year}-%m-%d"))
        .rename(
            columns=(
                c := {
                    "name": "Navn",
                    "address": "Adresse",
                    "birthday": "Fødselsdag",
                    "cpr": "Personnummer",
                }
            )
        )
        .filter(items=c.values())
    )

    # find residents with wedding anniversary next year
    df_anniversaries = (
        find_residents_with_wedding_anniversaries_for_year(
            anniversaries=[60, 65, 70, 75, 80],
            year=year,
        )
        .sort_values(by=["anniversary", "couple_id", "cpr"])
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
