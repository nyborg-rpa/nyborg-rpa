import os

import argh
import pandas as pd
from dotenv import load_dotenv
from tqdm.auto import tqdm

from nyborg_rpa.utils.email import send_email
from nyborg_rpa.utils.ms_graph import MSGraphClient
from nyborg_rpa.utils.pad import dispatch_pad_script

# only SKUs defined here will trigger notifications
NOTIFICATION_THRESHOLDS = {
    "M365EDU_A3_FACULTY": 3,  # Microsoft 365 A3 for Faculty
    "SPE_F1": 3,  # Microsoft 365 F3
    "SPE_E3": 5,  # Microsoft 365 E3
    "MCOEV": 3,  # Microsoft Teams Phone Standard
    "MCOEV_FACULTY": 3,  # Microsoft Teams Phone Standard for Faculty
}


# @task(ttl="1w", retries=3, directory=r"J:\Drift\59. MS License Monitor")
def fetch_sku_product_name_mapping() -> dict[str, str]:

    print("Fetching SKU ID to product name mapping...")
    return (
        pd.read_csv("https://download.microsoft.com/download/e/3/e/e3e9faf2-f28b-490a-9ada-c6089a1fc5b0/Product%20names%20and%20service%20plan%20identifiers%20for%20licensing.csv")
        .rename(columns={"String_Id": "skuId", "Product_Display_Name": "productName"})
        .set_index("skuId")["productName"]
        .to_dict()
    )


@argh.arg("--recipients", help="List of email recipients for the report.", nargs="*")
def ms_license_monitor(*, recipients: list[str]) -> None:

    load_dotenv(dotenv_path=r"J:\RPA\.baseflow\.env", override=True)

    # download SKU ID to product name mapping
    # https://learn.microsoft.com/en-us/entra/identity/users/licensing-service-plan-reference
    SKU_TO_PRODUCT_NAME_MAPPING = fetch_sku_product_name_mapping()

    # fetch SKUs from MS Graph API
    ms_graph_client = MSGraphClient(
        client_id=os.environ["MS_GRAPH_CLIENT_ID"],
        client_secret=os.environ["MS_GRAPH_CLIENT_SECRET"],
        tenant_id=os.environ["MS_GRAPH_TENANT_ID"],
    )

    print("Fetching subscribed SKUs from MS Graph API...")
    resp = ms_graph_client.get("/subscribedSkus")
    skus = resp.json().get("value", [])

    # process SKUs
    print("Processing SKUs...")
    rows = []
    for sku in tqdm(skus):

        sku_part = sku.get("skuPartNumber")
        product_name = SKU_TO_PRODUCT_NAME_MAPPING.get(sku_part, "")
        consumed = sku.get("consumedUnits", 0) or 0
        prepaid = sku.get("prepaidUnits", {}).get("enabled", 0) or 0
        free_units = prepaid - consumed

        rows += [
            {
                "productName": product_name,
                "skuPartNumber": sku_part,
                "prepaidUnits": prepaid,
                "consumedUnits": consumed,
                "freeUnits": free_units,
            }
        ]

    if not rows:
        print("No SKUs found.")
        return

    # add thresholds to dataframe
    df_skus = pd.DataFrame(rows).sort_values(by="freeUnits").reset_index(drop=True)
    df_skus["notificationThreshold"] = df_skus["skuPartNumber"].map(NOTIFICATION_THRESHOLDS).astype("Int64")

    # create alerts dataframe using notification thresholds
    df_alerts = df_skus.query("notificationThreshold.notnull() and freeUnits <= notificationThreshold").reset_index(drop=True)

    # send email notification if there are alerts
    if len(df_alerts):

        typography_style = "font-family: Arial, sans-serif; font-size: 12px"
        url = "https://entra.microsoft.com/#view/Microsoft_AAD_IAM/LicensesMenuBlade/~/Products"
        html_table = df_alerts.to_html(index=False)
        body = f"""<!DOCTYPE html>
        <html>
        <link rel="stylesheet" href="https://cdn.jupyter.org/notebook/5.1.0/style/style.min.css">
        <body style="margin:0; padding:0; {typography_style}; line-height:1.4;">
        <p>Følgende Microsoft 365 licenser er ved at løbe tør:</p>
        </br>
        {html_table}
        </br>
        <p>Du kan administrere licenserne i <a href="{url}">Microsoft Entra admin center</a>.</p>
        </br>
        <p>Venlig hilsen,<br>Robotten</p>
        </body>
        </html>"""

        send_email(
            sender=os.environ["MS_MAILBOX"],
            recipients=recipients,
            subject="MS365 licenser udløbsadvarsel",
            body=body,
            body_type="Html",
        )


if __name__ == "__main__":
    dispatch_pad_script(fn=ms_license_monitor)
