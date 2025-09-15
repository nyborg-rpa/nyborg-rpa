import json

import pandas as pd

from nyborg_rpa.utils.pad import dispatch_pad_script
from nyborg_rpa.utils.sharepoint import get_sharepoint_item_by_id


def helbredstillaeg_manual_mail_report(*, sharepoint_id: int, message: str | None = None) -> str:
    # This function generates a manual email report for the helbredstillaeg process.

    # Fetch from SharePoint item by ID
    sp_item = get_sharepoint_item_by_id(
        site="https://nyborg365.sharepoint.com/sites/RPADrift",
        list_="01.11.02 Helbredstillæg",
        id_=str(sharepoint_id),
    )

    output = json.loads(sp_item["Output"])
    cpr = str(sp_item["CPR"])
    treatment_type = str(sp_item["Behandlingsform"])
    treatment_date = pd.to_datetime(sp_item["Behandlingsdato"]).strftime("%Y-%m-%d")
    has_sygesikringsandel = bool(sp_item["HarSygesikringsandel_x003f_"])
    has_ydernummer = bool(sp_item["HarYdernummer_x003f_"])
    health_allowance_pct = f"{output["health_pct"]:.0%}"

    if message:
        output["status_message"] = message

    # Generate the standard message based on the status message
    match output["status_message"]:

        case "Der er ikke fundet en sag for behandlingen":
            msg = f"Robotten kunne ikke finde en {treatment_type} sag hos borgeren i KP, og er derfor sendt til manuel behandling"

        case "Tidligere udbetalt":
            msg = "Robotten fandt en tidligere udbetaling for samme behandling hos borgeren i KP, og er derfor sendt til manuel behandling"

        case "Fandt ikke borger i KP, muligvis fejl indtastet CPR i APP":
            msg = "Robotten kunne ikke finde borger i KP, muligvis fejl indtastet CPR nummer i App, og er derfor sendt til manuel behandling"

        case "Fandt ikke borger i KP":
            msg = "Robotten kunne ikke finde borger i KP, og er derfor sendt til manuel behandling"

        case "Der er fundet flere relevante sager":
            msg = "Robotten fandt flere relevante sager hos borger i KP, og er derfor sendt til manuel behandling"

        case "Indtastet behandlinger mangler beløb":
            msg = "Indtastet behandlinger har manglende beløb, og er derfor sendt til manuel behandling"

        case "Borgers helbredsprocent er 0":
            msg = "Robotten fandt ingen helbredsprocent hos borgeren i KP, og er derfor sendt til manuel behandling"

        case "Robotten kunne ikke finde borger i KP, og er derfor sendt til manuel behandling":
            msg = "Robotten kunne ikke finde borger i KP, og er derfor sendt til manuel behandling"

        case "Kunne ikke finde borgers Sygesikring Danmark medlemsstatus":
            msg = "Robotten kunne ikke finde borgerens medlems status af Sygesirking Danmark i KP, og er derfor sendt til manuel behandling"

        case "Dublet":
            msg = "Robotten har tidligere betalt samme kvittering, og vil derfor ikke behandle kvitteringen. Er derfor sendt til manuel behandling."

        case _:
            raise ValueError(f"ikke tilføjet!: {output["status_message"]}")

    # Generate the email body in HTML format
    body = """<!DOCTYPE html>
    <html lang="da">
    <head>
        <meta charset="UTF-8" />
        <style>
        body {
            font-family: Arial, sans-serif;
            background-color: #ffffff;
            margin: 0;
            padding: 0;
        }
        .container {
            max-width: 600px;
            margin: 30px auto;
            background-color: #ffffff;
            border-radius: 8px;
            padding: 30px;
            box-shadow: 0 4px 12px rgba(0, 0, 0, 0.1);
        }
        .container p {
            font-size: 14px;
            color: #333333;
            line-height: 1.6;
        }
        .header {
            font-size: 18px;
            font-weight: bold;
            margin-bottom: 20px;
            color: #2c3e50;
        }
        .section-title {
            font-size: 16px;
            font-weight: bold;
            margin-top: 30px;
            margin-bottom: 10px;
            border-bottom: 1px solid #e0e0e0;
            padding-bottom: 5px;
            color: #2c3e50;
        }
        .document {
            border: 1px solid #e0e0e0;
            border-radius: 6px;
            padding: 15px;
            margin-bottom: 15px;
            background-color: #ffffff;
        }
        .footer {
            margin-top: 30px;
            font-size: 13px;
            color: #555555;
        }
        </style>
    </head>"""

    body += (
        f"<body><div class='container'><p>Hej,</p><p>{msg}</p>"
        "<p class='section-title'>Sagsoplysninger</p>"
        f"<p><strong>{treatment_type} - </strong>{treatment_date}<br>"
        f"<strong>CPR: </strong> {cpr}<br>"
    )

    if "fod" in treatment_type.lower():
        body += f"<strong>sygesikringsandel: </strong> {has_sygesikringsandel}<br>"
        body += f"<strong>yder nummer: </strong> {has_ydernummer}<br>"

    body += (
        f"<strong>Fundet helbredsprocent: </strong> {health_allowance_pct}<br>"
        f"<strong>Fundet sygesikring danmark: </strong> {output["insurance_group"]}</p>"
        "<p class='section-title'>Behandlinger</p>"
    )

    for treatment in output["treatments"]:
        text = '<div class="document"><p>' f"""<strong>Behandling: </strong> {treatment['Behandling']}<br><strong>Pris: </strong> {treatment['Pris']} kr"""
        if "Tilskud" in treatment:
            text += f"<strong>Sygesikring Danmark tilskud: </strong> {treatment['Tilskud']} kr"
        text += "</p></div>"
        body += text

    # fmt: off
    body += (
        f"<p><strong>Beregnet tilskud: </strong> {output["total_price"]} kr</p>"
        "<p class='footer'>"
        "Venlig hilsen,<br>Robotten"
        "</p></div></body></html>"
    )  # fmt: on

    body = body.replace("\n", "")

    return body


if __name__ == "__main__":
    dispatch_pad_script(fn=helbredstillaeg_manual_mail_report)

    # example usage
    # sharepoint_id = 126
    # output = helbredstillaeg_manual_mail_report(sharepoint_id=sharepoint_id)
    # print(output)
