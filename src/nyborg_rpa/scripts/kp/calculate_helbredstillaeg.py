import json
import re
from datetime import datetime
from math import ceil
from typing import Literal, TypedDict

import argh
import pandas as pd

from nyborg_rpa.utils.pad import dispatch_pad_script


def parse_medical_insurance(
    *,
    medical_insurance: str,
) -> Literal["Gruppe 1", "Gruppe 2", "Gruppe 5", "Ikke medlem", "Ukendt"]:

    mapping = {
        "Gruppe 1": "Gruppe 1",
        "Gruppe 2": "Gruppe 2",
        "Gruppe 5": "Gruppe 5",
        "Ja - Basis (hvilende)": "Ikke medlem",
        "Nej": "Ikke medlem",
    }

    for name, value in mapping.items():
        if name.lower() in medical_insurance.lower():
            return value

    return "Ukendt"


class HelbredstillaegData(TypedDict):
    sharepoint_item_data: dict
    kp_data: dict
    sharepoint_treatments_df: pd.DataFrame
    kp_sagsoversigt_data_df: pd.DataFrame


def collect_data(*, sharepoint_id: int) -> HelbredstillaegData:
    """
    Collect data from SharePoint and KP for the given sharepoint_id.
    """
    # Declare paths
    sharepoint_item_path = f"J:/Drift/11. Helbredstillæg/{sharepoint_id}/sharepoint.json"
    sharepoint_treatments_path = f"J:/Drift/11. Helbredstillæg/{sharepoint_id}/sharepoint_treatments.json"
    kp_pensionsfakta_path = f"J:/Drift/11. Helbredstillæg/{sharepoint_id}/kp_pensionsfakta.json"
    kp_personoplysninger_path = f"J:/Drift/11. Helbredstillæg/{sharepoint_id}/kp_personoplysninger.json"
    kp_sagsoversigt_path = f"J:/Drift/11. Helbredstillæg/{sharepoint_id}/kp_sagsoversigt.json"
    kp_udbetaling_path = f"J:/Drift/11. Helbredstillæg/{sharepoint_id}/kp_udbetaling.json"

    # Loading json data
    sharepoint_item_data = json.load(open(sharepoint_item_path, "r", encoding="utf-8-sig"))
    kp_pensionsfakta_data = json.load(open(kp_pensionsfakta_path, "r", encoding="utf-8-sig"))
    kp_personoplysninger_data = json.load(open(kp_personoplysninger_path, "r", encoding="utf-8-sig"))
    kp_sagsoversigt_data = json.load(open(kp_sagsoversigt_path, "r", encoding="utf-8-sig"))
    kp_udbetaling_data = json.load(open(kp_udbetaling_path, "r", encoding="utf-8-sig"))
    sharepoint_treatments_data = json.load(open(sharepoint_treatments_path, "r", encoding="utf-8-sig"))

    # Combine kp data into a single dictionary
    kp_data = {
        "pensionsfakta": kp_pensionsfakta_data,
        "personoplysninger": kp_personoplysninger_data,
        "sagsoversigt": kp_sagsoversigt_data,
        "udbetaling": kp_udbetaling_data,
    }

    # Convert SharePoint item data to DataFrame
    rows = []
    for item in sharepoint_treatments_data["value"]:
        rows += [
            {
                "ID": item["ID"],
                "Behandlingsform": item["Behandlingsform"]["Value"],
                "Behandling": item["Behandling"],
                "MaksPris": item["MaksPris"],
                "Procent": item["Procent"],
                "År": item["OData__x00c5_r"]["Value"],
                "Grupper": [x["Value"] for x in item["Grupper"]],
            }
        ]

    sharepoint_treatments_df = pd.DataFrame(rows)

    # Convert kp sagsoversigt data to DataFrame
    rows = []
    for item in kp_data["sagsoversigt"]:
        rows += [
            {
                "Titel": item["Titel"],
                "Sagstype": item["Sagstype"],
                "Beviling start": pd.to_datetime(item["Beviling start"], format="%Y-%m-%d", errors="coerce"),
                "Beviling slut": pd.to_datetime(item["Beviling slut"], format="%Y-%m-%d", errors="coerce"),
                "Status": item["Status"],
            }
        ]

    kp_sagsoversigt_data_df = pd.DataFrame(rows)

    return {
        "sharepoint_item_data": sharepoint_item_data,
        "kp_data": kp_data,
        "sharepoint_treatments_df": sharepoint_treatments_df,
        "kp_sagsoversigt_data_df": kp_sagsoversigt_data_df,
    }


@argh.arg("--sharepoint-id", help="The SharePoint ID of the item to calculate health allowance for.")
def calculate_helbredstillaeg(*, sharepoint_id: int) -> dict:
    """
    Calculate the health allowance for a given SharePoint item.

    Args:
        sharepoint_id: The SharePoint ID of the item to calculate health allowance for.
    """

    data = collect_data(sharepoint_id=sharepoint_id)
    kp_data = data["kp_data"]
    sharepoint_item_data = data["sharepoint_item_data"]
    sharepoint_treatments_df = data["sharepoint_treatments_df"]
    kp_sagsoversigt_data_df = data["kp_sagsoversigt_data_df"]

    result = {
        "status": False,
        "status_message": "",
        "total_price": 0.0,
        "health_pct": 0.0,
        "insurance_group_denmark": "",
        "extended": False,
    }

    today = pd.Timestamp.now()
    treatment_date = pd.to_datetime(str(sharepoint_item_data["Behandlingsdato"]), format="%Y-%m-%d")

    # is the treatment date in the future?
    print("checking treatment date...")
    if treatment_date > today:
        result["status_message"] = "Behandlingsdato er i fremtiden!"
        print(f"failed: {result["status_message"]}")
        return result

    # is the treatment date older than 3 years?
    if treatment_date < today - pd.DateOffset(years=3):
        result["status_message"] = "Behandlingsdato er ældre end 3 år!"
        print(f"failed: {result["status_message"]}")
        return result

    # Extracting medical insurance group and Check if the medical insurance group is available
    print("checking medical insurance group...")
    medical_insurance_group = parse_medical_insurance(
        medical_insurance=kp_data["personoplysninger"]["Sygeforsikring danmark (gruppe)"]
    )

    result["insurance_group_denmark"] = medical_insurance_group

    if medical_insurance_group == "Ukendt":
        result["status_message"] = "Kunne ikke finde borgers Sygesikring Danmark medlemsstatus"
        print(f"failed: {result["status_message"]}")
        return result

    # Extracting data from SharePoint item
    treatments = json.loads(sharepoint_item_data["Behandlinger"])
    treatment_type = sharepoint_item_data["Behandlingsform"]["Value"]
    has_ydernummer = sharepoint_item_data["HarYdernummer_x003f_"]
    has_sygesikringsandel = sharepoint_item_data["HarSygesikringsandel_x003f_"]

    # calculate total price
    print("checking payment...")
    # TODO: husk kontrol af at der kun kan være en hovedbehandling pr. behandling
    for treatment in treatments:

        price = treatment["Pris"]
        result["total_price"] += price
        treatment_name = treatment["Behandling"]

        # exception for Fodbehandling
        if treatment_name == "Fodbehandling" and not has_ydernummer:
            continue

        # find treatment in available treatments
        found_treatment = sharepoint_treatments_df.query(
            f"Behandlingsform == {treatment_type!r} and Behandling == {treatment_name!r} and År == '{treatment_date.year}'"
        )

        if not len(found_treatment) == 1:
            raise ValueError(
                f"Der er ikke præcist ét match for behandlingen {treatment_name} i år {treatment_date.year}. Fundet: {len(found_treatment)}"
            )

        # does the insurance group match?
        found_treatment = found_treatment.to_dict(orient="records")[0]
        if medical_insurance_group not in found_treatment["Grupper"]:
            continue

        # calculate insurance part and subtract it from the total price
        insurance_part = min(price * found_treatment["Procent"], found_treatment["MaksPris"])
        result["total_price"] -= insurance_part

    # find the health allowance percentage that is valid for the treatment date
    health_allowance_pct = 0.0
    for item in kp_data["pensionsfakta"]["Helbredstillægsprocent"]:

        from_date = pd.to_datetime(str(item["Gyldig_Fra"]), format="%d-%m-%Y")
        to_date = pd.to_datetime(str(item["Gyldig_Til"]), format="%d-%m-%Y")

        if from_date <= treatment_date <= to_date:
            health_allowance_pct = float(item["Helbredsprocent"].strip("%")) / 100
            break

    # apply health allowance percentage
    result["total_price"] *= health_allowance_pct
    result["total_price"] = ceil(result["total_price"] * 100) / 100.0
    result["health_pct"] = health_allowance_pct
    print(f"calculate total price: {result["total_price"]}")

    if result["total_price"] == 0:
        result["status_message"] = "Borgers helbredsprocent er 0"
        print(f"failed: {result["status_message"]}")
        return result

    # find related KP cases based on treatment type and date

    match treatment_type:

        case "Fodbehandling":

            treatment_type_keyword = "fodb|fodp"

            case_type_keyword = "almindeligt helbredstillæg"
            if not has_sygesikringsandel:
                case_type_keyword = "udvidet helbredstillæg"
                result["extended"] = True

            is_udvidet_helbredstillæg = case_type_keyword == "udvidet helbredstillæg"
            action = "Standard" if is_udvidet_helbredstillæg ^ has_sygesikringsandel else "Manuel"

        case "Tandbehandling":

            treatment_type_keyword = "tand"
            # TODO: afklar hvad den skal søge efter for tandbehandling
            case_type_keyword = "almindeligt helbredstillæg"
            action = "Standard"

    # search for cases in the KP sagsoversigt data
    print("checking for available case...")
    found_cases = kp_sagsoversigt_data_df.query(
        f"Titel.str.lower().str.contains({treatment_type_keyword!r}, regex=True) and "
        f"Sagstype.str.lower().str.contains({case_type_keyword!r}, regex=True) and"
        f"`Beviling start` < '{treatment_date}' and "
        f"(`Beviling slut`.isna() or `Beviling slut` > '{treatment_date}')",
        engine="python",
    )

    if not len(found_cases):
        result["status_message"] = "Der er ikke fundet en sag for behandlingen"
        print(f"failed: {result["status_message"]}")
        return result

    # check if the treatment has been paid out before
    print("checking previous payment...")
    for item in kp_data["udbetaling"]:
        if treatment_type_keyword not in item["Navn"].lower():
            continue
        # TODO: tilføj dmy uden noget?
        item_date_text = item["Navn"]
        date_fmts = ("%d-%m-%Y", "%d-%m-%y", "%d.%m.%Y", "%d.%m.%y", "%d/%m/%Y", "%d/%m/%y", "%d%m%Y", "%d%m%y")
        item_price_float = float(item["Beløb"].replace("\xa0kr.", "").replace(".", "").replace(",", ".").strip())
        found_date = next(
            (
                x
                for fmt in date_fmts
                if (x := pd.to_datetime(item_date_text, format=fmt, exact=False, errors="coerce")) is not pd.NaT
            ),
            None,
        )
        if not found_date:
            if result["total_price"] != item_price_float:
                continue
            else:
                result["status_message"] = "Måske tidligere udbetalt"
                print(f"failed: {result["status_message"]}")
                return result
        if found_date != treatment_date:
            continue
        result["status_message"] = "Tidligere udbetalt"
        print(f"failed: {result["status_message"]}")
        return result

    # prepare the result
    result["status_message"] = action
    result["status"] = action != "Manuel"
    result["total_price"] = 0.0 if action == "Manuel" else result["total_price"]
    result["found_cases"] = found_cases.to_dict(orient="records")[0]

    if not result["status"]:
        print(f"failed: {result["status_message"]}")
    # TODO: print result smart
    return result


if __name__ == "__main__":
    dispatch_pad_script(fn=calculate_helbredstillaeg)

    # Example usage
    # sharepoint_id = 20  # Replace with the actual SharePoint ID
    # status = calculate_helbredstillaeg(sharepoint_id=sharepoint_id)
    # print(status)
