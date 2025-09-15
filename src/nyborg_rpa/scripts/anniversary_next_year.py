import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from xlsxwriter.worksheet import Worksheet

from nyborg_rpa.utils.datafordeler import DatafordelerClient
from nyborg_rpa.utils.email import send_email
from nyborg_rpa.utils.excel import df_to_excel_table

datafordeler_client: DatafordelerClient


def wedding_anniversary_to_excel_table(*, df: pd.DataFrame, filepath: Path | str, sheet_name: str = "Sheet1") -> None:
    """Write a pandas DataFrame to an Excel file as a formatted table with autofit columns."""

    filepath = Path(filepath)
    writer = pd.ExcelWriter(filepath, engine="xlsxwriter")
    df.to_excel(writer, sheet_name=sheet_name, startrow=1, header=False, index=False)

    worksheet: Worksheet = writer.sheets[sheet_name]
    (rows, cols) = df.shape
    column_settings = [{"header": column} for column in df.columns]

    worksheet.add_table(0, 0, rows, cols - 1, {"columns": column_settings, "style": "Table Style Medium 2", "banded_rows": False})
    worksheet.set_column(0, cols - 1, 1)
    worksheet.autofit()
    worksheet.set_column("A:A", None, None, {"hidden": True})

    row_idx = 1
    for i, grp in df.groupby("id"):
        color = "#FFFFFF" if i % 2 == 0 else "#DCE6F1"
        cell_format = writer.book.add_format({"bg_color": color})
        for _ in range(len(grp)):
            # Only apply background color to data columns (not the entire row)
            worksheet.set_row(row_idx, None)
            worksheet.conditional_format(row_idx, 0, row_idx, cols - 1, {"type": "no_blanks", "format": cell_format})
            row_idx += 1
        worksheet.write_blank(row_idx - 1, 1, None, cell_format)
        worksheet.write_blank(row_idx - 1, 2, None, cell_format)

    writer.close()


def get_wedding_anniversary(*, anniversary: int, year: int, url: str) -> pd.DataFrame:
    params = {
        "civ.status.eq": "aktuel",
        "civ.civilstandstype.eq": "gift",
        "civ.virkningtil.eq": None,
        "civ.virkningfra.ge": f"{year - anniversary}-01-01",
        "civ.virkningfra.le": f"{year - anniversary}-12-31",
        "cadr.cprkommunekode.eq": "450",
        "adropl.fraflytningskommunekode.ne": "450",
        "person.status.wi": "bopael_i_danmark|bopael_i_danmark_hoej_vejkode",
    }
    data = datafordeler_client.get(url=url, params=params)
    citizens_data = datafordeler_client.fech_citizens_data(data=data)
    if not citizens_data:
        return pd.DataFrame()  # Return an empty DataFrame if no data is found

    df = pd.DataFrame(citizens_data)
    df["couple_key"] = df[["cpr", "partner_cpr"]].apply(lambda x: tuple(sorted([x["cpr"], x["partner_cpr"]])) if pd.notna(x["partner_cpr"]) else (x["cpr"],), axis=1)
    df["anniversary"] = f"{anniversary} år"

    return df


def anniversary_next_year(working_dir: Path | str, mail_recipients: list[str]):
    working_dir: Path = Path(working_dir)
    citizens_wedding_anniversary_file = working_dir / "citizens_wedding_anniversary.xlsx"
    citizens_hundred_years_file = working_dir / "citizens_hundred_years.xlsx"

    url = "https://s5-certservices.datafordeler.dk/CPR/CPRPersonFullComplete/1/REST/PersonFullCurrentListComplete"
    today = datetime.today()
    year = today.year + 1

    global datafordeler_client

    datafordeler_client = DatafordelerClient()

    # Get citizens with 100 years birthday next year
    params = {
        "person.foedselsdato.ge": f"{year - 100}-01-01",
        "person.foedselsdato.le": f"{year - 100}-12-31",
        "cadr.cprkommunekode.eq": "450",
        "adropl.fraflytningskommunekode.ne": "450",
        "person.status.wi": "bopael_i_danmark|bopael_i_danmark_hoej_vejkode",
    }
    data = datafordeler_client.get(url=url, params=params)
    citizens_hundred_years = datafordeler_client.fech_citizens_data(data=data)

    df_hundred_years = pd.DataFrame(citizens_hundred_years)
    df_hundred_years["Fødselsdag"] = f"{year}-" + pd.to_datetime(df_hundred_years["birthday"]).dt.strftime("%m-%d")
    df_hundred_years = df_hundred_years.sort_values("Fødselsdag").drop(columns=["birthday", "civil_status", "partner_cpr", "civil_valid_from"])

    cols = df_hundred_years.columns.tolist()
    cols = ["name"] + ["address"] + ["Fødselsdag"] + ["cpr"]
    df_hundred_years = df_hundred_years[cols]

    df_hundred_years = df_hundred_years.rename(columns={"name": "Fulde navn", "address": "Adresse", "cpr": "Personnummer"})

    df_to_excel_table(df=df_hundred_years, filepath=citizens_hundred_years_file, sheet_name="hundred_years")

    # Get citizens with wedding anniversary next year
    anniversary_years = [60, 65, 70, 75, 80]
    dfs = [get_wedding_anniversary(anniversary=a, year=year, url=url) for a in anniversary_years]
    df_all_anniversary = pd.concat(dfs, ignore_index=True)

    # add id column by enumerating couple_key
    df_all_anniversary["id"] = df_all_anniversary["couple_key"].astype("category").cat.codes
    # antag at df er din DataFrame
    cols = df_all_anniversary.columns.tolist()
    cols = (
        ["id"]
        + ["anniversary"]
        + ["civil_valid_from"]
        + ["name"]
        + ["address"]
        + ["birthday"]
        + ["cpr"]
        + [c for c in cols if (c not in ["id", "anniversary", "civil_valid_from", "name", "address", "birthday", "cpr"])]
    )

    df_all_anniversary = df_all_anniversary[cols]

    df_all_anniversary = df_all_anniversary.sort_values("id").drop(columns=["couple_key", "civil_status", "partner_cpr"])
    df_all_anniversary = df_all_anniversary.rename(
        columns={"anniversary": "Juilæum", "civil_valid_from": "Vielsesdaton", "name": "Fulde navn", "address": "Adresse", "birthday": "Fødselsdato", "cpr": "Personnummer"}
    )

    # from nyborg_rpa.utils.excel import df_to_excel_table
    wedding_anniversary_to_excel_table(df=df_all_anniversary, filepath=citizens_wedding_anniversary_file, sheet_name="wedding_anniversary")

    # Send mail with excel file as attachment
    load_dotenv(override=True)

    typography_style = "font-family: Arial, sans-serif; font-size: 12px"
    body = f"""<!DOCTYPE html>
    <html>
    <body style="margin:0; padding:0; {typography_style}; line-height:1.4;">
    <p>Vedhæftet finder du <strong>citizens_hundred_years.xlsx</strong> med kommende 100 års fødselsdage og <strong>citizens_wedding_anniversary.xlsx</strong> med bryllup jubilæums.</p>
    <p>Venlig hilsen,<br>Robotten</p>
    </body>
    </html>"""

    send_email(
        sender=os.environ["MS_MAILBOX"],
        recipients=mail_recipients,
        subject=f"Levering af filer: {year} - 100 år og Bryllup jubilæum",
        body=body,
        attachments=[citizens_hundred_years_file, citizens_wedding_anniversary_file],
    )

    # cleanup
    (citizens_wedding_anniversary_file).unlink()
    (citizens_hundred_years_file).unlink()


if __name__ == "__main__":
    user = ""
    anniversary_next_year(working_dir=f"C:/Users/{user}/Downloads", mail_recipients=[f"{user}@nyborg.dk"])
