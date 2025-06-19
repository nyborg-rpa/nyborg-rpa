import json
import re
from decimal import Decimal
from math import ceil
from pathlib import Path
from typing import Literal, TypedDict

import argh
import pandas as pd

from nyborg_rpa.utils.pad import dispatch_pad_script


class HelbredstillaegData(TypedDict):
    sharepoint_item: dict
    sharepoint_treatments: dict
    kp: dict
    sharepoint_treatments_df: pd.DataFrame
    kp_sagsoversigt_df: pd.DataFrame


def read_json(path: str) -> dict:
    """Read JSON file and return its content."""
    with open(path, "r", encoding="utf-8-sig") as file:
        return json.load(file)


def fetch_data(*, sharepoint_id: int) -> HelbredstillaegData:
    """
    Fetch and parse data from SharePoint and KP for the given `sharepoint_id`.
    """

    base_path = Path(f"J:/Drift/11. Helbredstillæg/{sharepoint_id}")
    assert base_path.exists(), f"{base_path=} does not exist."

    # load data into a dictionary for the given item_id
    data = {
        "sharepoint_item": read_json(base_path / "sharepoint.json"),
        "sharepoint_treatments": read_json(base_path / "sharepoint_treatments.json"),
        "kp": {
            "pensionsfakta": read_json(base_path / "kp_pensionsfakta.json"),
            "personoplysninger": read_json(base_path / "kp_personoplysninger.json"),
            "sagsoversigt": read_json(base_path / "kp_sagsoversigt.json"),
            "udbetaling": read_json(base_path / "kp_udbetaling.json"),
        },
    }

    # parse available treatments into a DataFrame
    data["sharepoint_treatments_df"] = pd.DataFrame(
        {
            "ID": item["ID"],
            "Behandlingsform": item["Behandlingsform"]["Value"],
            "Behandling": item["Behandling"],
            "MaksPris": item["MaksPris"],
            "Procent": item["Procent"],
            "År": item["OData__x00c5_r"]["Value"],
            "Grupper": [x["Value"] for x in item["Grupper"]],
        }
        for item in data["sharepoint_treatments"]["value"]
    )

    # parse KP sagsoversigt data into a DataFrame
    data["kp_sagsoversigt_df"] = pd.DataFrame(
        {
            "Titel": item["Titel"],
            "Sagstype": item["Sagstype"],
            "Beviling start": pd.to_datetime(item["Beviling start"], format="%Y-%m-%d", errors="coerce"),
            "Beviling slut": pd.to_datetime(item["Beviling slut"], format="%Y-%m-%d", errors="coerce"),
            "Status": item["Status"],
        }
        for item in data["kp"]["sagsoversigt"]
    )

    return data


def parse_insurance_group(text: str) -> Literal["Gruppe 1", "Gruppe 2", "Gruppe 5", "Ikke medlem"] | None:
    """Parse the insurance group from the given `text`, e.g. `'gruppde 1 Danmark'` to `'Gruppe 1'`."""

    mapping = {
        "Gruppe 1": "Gruppe 1",
        "Gruppe 2": "Gruppe 2",
        "Gruppe 5": "Gruppe 5",
        "Ja - Basis (hvilende)": "Ikke medlem",
        "Nej": "Ikke medlem",
    }

    for name, value in mapping.items():
        if name.lower() in text.lower():
            return value


def calculate_helbredstillaeg_for_case(data: HelbredstillaegData) -> dict:

    # TODO: change to a "case"-based approach
    # where case is a dict with all the necessary data and is updated with each step
    # with fetch_case(), fetch_available_treatments(), and fetch_kp_data() functions

    output = {
        "status": False,  # can we payout the case?
        "status_message": "",
        "total_price": 0.0,
        "health_pct": 0.0,
        "insurance_group": None,
        "extended": False,
    }

    # #️ STEP 1
    # check insurance group and treatment date

    # parse insurance group from KP and check if it is available
    print("checking medical insurance group...")
    if not (grp := parse_insurance_group(data["kp"]["personoplysninger"]["Sygeforsikring danmark (gruppe)"])):
        output["status_message"] = "Kunne ikke finde borgers Sygesikring Danmark medlemsstatus"
        return output
    else:
        output["insurance_group"] = grp

    # check treatment date
    print("checking treatment date...")
    today = pd.Timestamp.now()
    treatment_date = pd.to_datetime(str(data["sharepoint_item"]["Behandlingsdato"]), format="%Y-%m-%d")

    # is the treatment date in the future?
    if treatment_date > today:
        output["status_message"] = "Behandlingsdato er i fremtiden!"
        return output

    # is the treatment date older than 3 years?
    if treatment_date < today - pd.DateOffset(years=3):
        output["status_message"] = "Behandlingsdato er ældre end 3 år!"
        return output

    # #️⃣ STEP 2
    # calcuate the insurance part for each treatment

    # parse treatment data
    treatments: list[dict] = json.loads(data["sharepoint_item"]["Behandlinger"])
    treatment_type = str(data["sharepoint_item"]["Behandlingsform"]["Value"])
    has_ydernummer = bool(data["sharepoint_item"]["HarYdernummer_x003f_"])
    has_sygesikringsandel = bool(data["sharepoint_item"]["HarSygesikringsandel_x003f_"])

    # treatments can only contain max one main treatment per treatment type
    if treatment_type == "Fodbehandling":
        main_treatments = [x["Behandling"] for x in treatments if re.match(r"^Behandlingstype\s[ABC]$|^Almindelig$", x["Behandling"])]
        if len(main_treatments) > 1:
            raise ValueError(f"Found >1 main treatments for {treatment_type} on {treatment_date:%Y-%m-%d}: {main_treatments}")

    # calculate total price
    print(f"checking {len(treatments)} treatment(s) for {treatment_type} on {treatment_date:%Y-%m-%d}...")
    output["treatments"] = treatments  # save for later
    for treatment in treatments:

        price = float(treatment["Pris"])
        treatment_name = str(treatment["Behandling"])
        output["total_price"] += price

        # if the treatment is Fodbehandling, the patient must have a Ydernummer
        if treatment_name == "Fodbehandling" and not has_ydernummer:
            treatment["Tilskud"] = None
            continue

        # lookup treatment in available treatments (there should be exactly one match)
        # fmt: off
        found_treatments = data["sharepoint_treatments_df"].query(
            f"Behandlingsform == {treatment_type!r}"
            " and "
            f"Behandling == {treatment_name!r}"
            " and "
            f"År == '{treatment_date.year}'"
        )  # fmt: on

        if len(found_treatments) != 1:
            raise ValueError(f"Found >1 matches for {treatment_type}/{treatment_name} on {treatment_date.year}: {list(found_treatments["Behandling"])}")

        # extract the only match
        found_treatment = found_treatments.iloc[0].to_dict()

        # does the patient have a valid insurance group for this treatment?
        if output["insurance_group"] not in found_treatment["Grupper"]:
            continue

        # calculate insurance part and subtract it from the total price
        insurance_part = min(price * found_treatment["Procent"], found_treatment["MaksPris"])
        treatment["Tilskud"] = insurance_part
        output["total_price"] -= insurance_part

    # #️ STEP 3
    # apply health allowance percentage

    # find the health allowance percentage that is valid for the treatment date
    health_allowance_pct = 0.0
    for item in data["kp"]["pensionsfakta"]["Helbredstillægsprocent"]:

        from_date = pd.to_datetime(str(item["Gyldig_Fra"]), format="%d-%m-%Y")
        to_date = pd.to_datetime(str(item["Gyldig_Til"]), format="%d-%m-%Y")

        if from_date <= treatment_date <= to_date:
            health_allowance_pct = float(item["Helbredsprocent"].strip("%")) / 100
            break

    # apply health allowance percentage
    output["total_price"] *= health_allowance_pct
    output["total_price"] = ceil(output["total_price"] * 100) / 100.0  # round up to nearest 0.01
    output["health_pct"] = health_allowance_pct

    if output["total_price"] == 0:
        output["status_message"] = "Borgers helbredsprocent er 0"
        return output

    # #️ STEP 4
    # check if the treatment is already paid out

    # find related KP cases based on treatment type and date
    match treatment_type:

        case "Fodbehandling":

            treatment_type_keyword = "fodb|fodp"
            case_type_keyword = "almindeligt helbredstillæg"

            if not has_sygesikringsandel:
                case_type_keyword = "udvidet helbredstillæg"
                output["extended"] = True

        case "Tandbehandling":

            treatment_type_keyword = "tand"
            case_type_keyword = "almindeligt helbredstillæg"

    print(f"searching for related KP cases using {treatment_type_keyword=!r} and {case_type_keyword=!r} on {treatment_date:%Y-%m-%d}...")
    kp_cases = data["kp_sagsoversigt_df"].query(
        f"Titel.str.lower().str.contains({treatment_type_keyword!r}, regex=True)"
        " and "
        f"Sagstype.str.lower().str.contains({case_type_keyword!r}, regex=True)"
        " and "
        f"`Beviling start` < '{treatment_date}'"
        " and "
        f"(`Beviling slut`.isna() or `Beviling slut` > '{treatment_date}')",
        engine="python",
    )

    if not len(kp_cases):
        output["status_message"] = "Der er ikke fundet en sag for behandlingen"
        return output

    output["found_cases"] = kp_cases.to_dict(orient="records")
    if len(kp_cases) != 1:
        raise ValueError(f"Found >1 KP cases for {treatment_type} on {treatment_date:%Y-%m-%d}: {list(kp_cases['Titel'])}")

    # check all previous payments to see if the current case has been paid out before
    print("checking if case has been paid out before...")
    for payment in data["kp"]["udbetaling"]:

        # is the payment is related to the treatment type?
        if not re.search(treatment_type_keyword, payment["Navn"].lower()):
            continue

        # parse the treatment date from the payment name
        # the payment name is typically in the format "Behandling d. 01-01-2023"
        date_fmts = ("%d-%m-%Y", "%d-%m-%y", "%d.%m.%Y", "%d.%m.%y", "%d/%m/%Y", "%d/%m/%y", "%d%m%Y", "%d%m%y")
        found_date = next((x for fmt in date_fmts if (x := pd.to_datetime(payment["Navn"], format=fmt, exact=False, errors="coerce")) is not pd.NaT), None)

        if found_date == treatment_date:
            output["status_message"] = "Tidligere udbetalt"
            return output

        # if we didn't find a date, but the total price matches the item price,
        # it might have been paid out before, but we don't know the date
        # so we assume it is a manual case
        item_price = Decimal(payment["Beløb"].replace("\xa0kr.", "").replace(".", "").replace(",", ".").strip())
        if not found_date and Decimal(output["total_price"]) == item_price:
            output["status_message"] = "Måske tidligere udbetalt"
            return output

    # #️ STEP 5
    # if we reach this point, it means that the treatment is valid and not paid out before

    # everything is fine, we can proceed with the action
    output["status_message"] = "Standard"
    output["status"] = True

    return output


@argh.arg("--sharepoint-id", help="The SharePoint ID of the item to calculate health allowance for.")
def calculate_helbredstillaeg(*, sharepoint_id: int) -> dict:
    """
    Calculate the health allowance for a given SharePoint item.

    Args:
        sharepoint_id: The SharePoint ID of the item to calculate health allowance for.
    """

    data = fetch_data(sharepoint_id=sharepoint_id)
    output = calculate_helbredstillaeg_for_case(data)

    if output["status"] is False:
        print(f"Error: {output["status_message"]}")

    return output


if __name__ == "__main__":
    dispatch_pad_script(fn=calculate_helbredstillaeg)

    # example usage
    # sharepoint_id = 8
    # output = calculate_helbredstillaeg(sharepoint_id=sharepoint_id)
    # print(output)
