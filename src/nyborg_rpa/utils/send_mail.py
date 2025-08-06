import os
from typing import Literal

import requests
from dotenv import load_dotenv


def send_email(
    *,
    sender: str,
    recipients: list[str],
    subject: str = "",
    body: str = "",
    body_type: Literal["Text", "Html"] = "Html",
):

    assert body_type in ["Text", "Html"], "body_type must be either 'Text' or 'Html'"
    assert isinstance(recipients, list) and all(isinstance(r, str) for r in recipients), "Recipients must be a list of strings"
    assert isinstance(sender, str), "Sender must be a string"

    load_dotenv(override=True)

    tenant_id = os.getenv("MS_GRAPH_TENANT_ID")
    client_id = os.getenv("MS_GRAPH_CLIENT_ID")
    client_secret = os.getenv("MS_GRAPH_CLIENT_SECRET")

    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://graph.microsoft.com/.default",
    }

    response = requests.post(url, data=data)
    access_token = response.json().get("access_token")

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    body = {
        "message": {
            "subject": subject,
            "body": {"contentType": body_type, "content": body},
            "toRecipients": [{"emailAddress": {"address": recipient}} for recipient in recipients],
        },
        "saveToSentItems": True,
    }

    resp = requests.post(url=f"https://graph.microsoft.com/v1.0/users/{sender}/sendMail", headers=headers, json=body)
    resp.raise_for_status()
