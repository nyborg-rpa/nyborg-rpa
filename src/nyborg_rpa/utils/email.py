import base64
import mimetypes
import os
from pathlib import Path
from typing import Iterable, Literal

import requests
from dotenv import load_dotenv

# Microsoft Graph har en praktisk grænse på ca. 3 MB pr. fileAttachment.
# Større filer kræver upload session (resumable upload).
GRAPH_SMALL_ATTACHMENT_MAX = 3 * 1024 * 1024  # 3 MB


def _file_to_graph_attachment(path: Path) -> dict:
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"Filen findes ikke: {path}")
    size = path.stat().st_size
    if size > GRAPH_SMALL_ATTACHMENT_MAX:
        raise ValueError(f"Vedhæftning er for stor til 'fileAttachment' ({size} bytes). " "Brug upload session til store filer.")
    with path.open("rb") as f:
        content_b64 = base64.b64encode(f.read()).decode("utf-8")

    # MIME-type er ikke påkrævet af Graph til fileAttachment, men fint at sende som hint.
    mime, _ = mimetypes.guess_type(str(path))
    return {
        "@odata.type": "#microsoft.graph.fileAttachment",
        "name": path.name,
        "contentType": mime or "application/octet-stream",
        "contentBytes": content_b64,
    }


def send_email(
    *,
    sender: str,
    recipients: list[str],
    subject: str = "",
    body: str = "",
    body_type: Literal["Text", "Html"] = "Html",
    attachments: Iterable[str] | None = None,
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

    message: dict = {
        "subject": subject,
        "body": {"contentType": body_type, "content": body},
        "toRecipients": [{"emailAddress": {"address": r}} for r in recipients],
    }

    # Tilføj vedhæftninger hvis angivet
    if attachments:
        att_objs = []
        for p in attachments:
            att_objs.append(_file_to_graph_attachment(Path(p)))
        message["attachments"] = att_objs

    body = {
        "message": message,
        "saveToSentItems": True,
    }

    resp = requests.post(url=f"https://graph.microsoft.com/v1.0/users/{sender}/sendMail", headers=headers, json=body)
    resp.raise_for_status()
