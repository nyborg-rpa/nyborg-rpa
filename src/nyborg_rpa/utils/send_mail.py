import os
from typing import Literal

import requests
from dotenv import load_dotenv


def send_email(
    *,
    to_addr: str,
    from_addr: str,
    subject: str = "",
    body: str = "",
    body_type: Literal["Text", "Html"] = "Html",
):

    load_dotenv(override=True)

    tenant_id = os.getenv("MS_GRAPH_TENANT_ID")
    client_id = os.getenv("MS_GRAPH_CLIENT_ID")
    client_secret = os.getenv("MS_GRAPH_CLIENT_SECRET")

    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    data = {"grant_type": "client_credentials", "client_id": client_id, "client_secret": client_secret, "scope": "https://graph.microsoft.com/.default"}

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
            "toRecipients": [{"emailAddress": {"address": to_addr}}],
        },
        "saveToSentItems": True,
    }

    resp = requests.post(url=f"https://graph.microsoft.com/v1.0/users/{from_addr}/sendMail", headers=headers, json=body)
    resp.raise_for_status()
