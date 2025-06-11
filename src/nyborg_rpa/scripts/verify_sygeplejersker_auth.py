from io import StringIO
from pathlib import Path
from typing import Literal

import argh
import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm.auto import tqdm

from nyborg_rpa.utils.pad import dispatch_pad_script

print = tqdm.write


def fetch_sygeplejerske_auth(auth_id: str) -> dict:
    """Fetch authorization details for a sygeplejerske by their authorization ID."""

    resp = requests.get(
        url=f"https://autregweb.sst.dk/Authorization.aspx?id={auth_id}",
        timeout=5,
    )
    resp.raise_for_status()

    # extract table <table class="Practitioner">
    soup = BeautifulSoup(resp.text, "html.parser")
    tbl = soup.find("table", class_="Practitioner")
    values = [row.text.split(":")[-1].strip() for row in tbl.find_all("tr")]

    return {
        "id": resp.url.split("id=")[-1],
        "authorization_status": values[1],
        "first_names": values[2],
        "last_name": values[3],
        "birthdate": pd.to_datetime(values[4], format="%d-%m-%Y"),
        "profession": values[5],
        "authorization_date": values[6],
        "authorization_id": values[7],
        "education_country": values[8],
    }


def verify_sygeplejerske_auth(
    *,
    name: str,
    birthdate_ddmmyy: str,
) -> Literal["Gyldig", "Ugyldig", "Manuel"]:

    name = name.strip().lower()
    birthdate = pd.to_datetime(birthdate_ddmmyy, format="%d%m%y")

    # if birthdate is in the future, assume it's a mistake and use 100 years ago
    if birthdate > pd.Timestamp.today():
        birthdate = birthdate.replace(year=birthdate.year - 100)

    print(f"Verifying sygeplejerske {name=!r} with {birthdate=:'%Y-%m-%d'}...")

    first_names = name.split(" ")[:-1]  # all but the last name
    last_name = name.split(" ")[-1]  # the last name

    ids = set()
    url = "https://autregweb.sst.dk/AuthorizationSearchResult.aspx"
    params = {
        "name": f"{first_names[0]} {last_name}",
        "birthmin": birthdate.strftime("%d%m%Y"),
    }

    resp = requests.get(url, params=params, allow_redirects=False, timeout=5)
    resp.raise_for_status()
    markup = resp.text

    # if there's a direct match, we get redirected to a direct id link
    if resp.status_code == 302:
        ids |= {resp.headers["Location"].split("id=")[-1]}

    # no results found?
    elif "Søgningen gav ingen resultater" in markup:
        print(f"No results found for {name=!r} with {birthdate=:'%Y-%m-%d'}")
        return "Manuel"

    # we got a table of possible matches
    else:
        soup = BeautifulSoup(markup, "html.parser")
        tbl = soup.find("div", class_="ClientSearchResults").find("table")
        rows = tbl.find_all("tr")[1:]
        ids |= {row.find("a")["href"].split("id=")[-1] for row in rows}

    # fetch metadata for each id
    metadatas = [fetch_sygeplejerske_auth(id_) for id_ in ids]
    data = pd.DataFrame(metadatas)
    data["name"] = data["first_names"] + " " + data["last_name"]

    # try to filter by full name
    if name in set(data["name"].str.lower()):
        data = data.query(f"name.str.lower() == {name!r}")

    if len(data) > 1:
        assert data["name"].nunique() == 1, "Multiple candidates found with the same name."

    # verify the authorization status, but only if we are sure about the name
    if (
        next(x for x in data["name"].str.lower()) == name
        or next(x for x in data["last_name"].str.lower()) == last_name
        and next(x for x in data["first_names"].str.lower()).split()[0] == first_names[0]
        and next(x for x in data["birthdate"]) == birthdate
    ):
        auth_status = next(x for x in data["authorization_status"])
        status = "Gyldig" if auth_status == "Autorisation gyldig." else "Ugyldig"

    else:
        status = "Manuel"

    return status


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


@argh.arg("--filepath", help="Path to the CSV export from SD Løn")
@argh.arg("--output-dir", help="Directory to save the report files.")
def verify_sygeplejersker_auth(
    *,
    filepath: Path | str,
    output_dir: Path | str,
):

    # check args
    filepath = Path(filepath)
    output_dir = Path(output_dir)

    if not filepath.exists():
        raise FileNotFoundError(f"File {filepath} does not exist.")

    # load the CSV file
    with open(filepath, "r", encoding="ansi") as f:

        text = f.read()

        # assert that the 25th line starts with "Navn (for-/efternavn)"
        expected_header = '="Afdeling (Ny 4)";="Tjenestenummer";="CPR-nummer";="Navn (for-/efternavn)";="Stillingskode nuværende";="Stilling"'
        assert text.splitlines()[24] == expected_header, "CSV header does not match expected format."

        # read csv, replace =" and " with empty string, and load into DataFrame
        text = text.replace('="', "").replace('"', "").replace("=", "")
        df = pd.read_csv(StringIO(text), skiprows=24, sep=";")

    # perform verification for each sygeplejerske (row in DataFrame) and add a "Status" column
    for idx, row in tqdm(df.iterrows(), desc="Verifying sygeplejersker", total=len(df)):
        df.loc[idx, "Status"] = verify_sygeplejerske_auth(
            name=row["Navn (for-/efternavn)"],
            birthdate_ddmmyy=row["CPR-nummer"].split("-")[0],
        )

    # generate the report DataFrame
    report = (
        df.assign(
            Fødselsdag=lambda x: x["CPR-nummer"].str.replace(r"^(\d{2})(\d{2})(\d{2}).*$", r"\1-\2-\3", regex=True)
        )
        .filter(
            items=[
                "Navn (for-/efternavn)",
                "Fødselsdag",
                "Stilling",
                "Stillingskode nuværende",
                "Status",
            ]
        )
        .rename(
            columns={
                "Navn (for-/efternavn)": "Navn",
                "Stilling": "Stillingsbetegnelse",
                "Stillingskode nuværende": "Stillingskode",
            }
        )
    )

    # save the report to Excel
    print(f"Saving Sygeplejersker auth reports to {output_dir.resolve().absolute().as_posix()}...")

    to_excel(
        df=report,
        filepath=output_dir / "sygeplejersker_auth_report.xlsx",
        sheet_name="Kontrol",
    )

    to_excel(
        df=report.query("Status != 'Gyldig'"),
        filepath=output_dir / "sygeplejersker_auth_report_invalid.xlsx",
        sheet_name="Kontrol",
    )


if __name__ == "__main__":
    dispatch_pad_script(fn=verify_sygeplejersker_auth)
