import base64
import mimetypes
import os
from datetime import datetime
from pathlib import Path
from typing import Iterable, Literal

import requests
from dotenv import load_dotenv

EMAIL_ATTACHMENT_MAX_SIZE_BYTES = 3 * 1024 * 1024  # 3 MB
"""Microsoft Graph file attachment file size limit."""


def get_token() -> str:
    load_dotenv(override=True)

    tenant_id = os.getenv("MS_GRAPH_TENANT_ID")
    client_id = os.getenv("MS_GRAPH_CLIENT_ID")
    client_secret = os.getenv("MS_GRAPH_CLIENT_SECRET")

    assert tenant_id, "Environment variable MS_GRAPH_TENANT_ID is not set"
    assert client_id, "Environment variable MS_GRAPH_CLIENT_ID is not set"
    assert client_secret, "Environment variable MS_GRAPH_CLIENT_SECRET is not set"

    # fetch access token
    resp = requests.post(
        url=f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "https://graph.microsoft.com/.default",
        },
    )

    resp.raise_for_status()
    access_token = resp.json()["access_token"]

    return access_token


def convert_file_to_graph_attachment(filepath: Path | str) -> dict:

    filepath = Path(filepath)
    if not filepath.exists() or not filepath.is_file():
        raise FileNotFoundError(f"The file {filepath.as_posix()!r} does not exist.")

    filesize = filepath.stat().st_size
    if filesize > EMAIL_ATTACHMENT_MAX_SIZE_BYTES:
        raise ValueError(f"The file {filepath.as_posix()!r} ({filesize} bytes) exceeds the maximum allowed size of {EMAIL_ATTACHMENT_MAX_SIZE_BYTES} bytes.")

    # read file, encode to base64, and infer mime type
    content_b64 = base64.b64encode(filepath.read_bytes()).decode("utf-8")
    mime, _ = mimetypes.guess_type(filepath)

    return {
        "@odata.type": "#microsoft.graph.fileAttachment",
        "name": filepath.name,
        "contentType": mime or "application/octet-stream",  # not required, but good to have
        "contentBytes": content_b64,
    }


def send_email(
    *,
    sender: str,
    recipients: list[str],
    subject: str = "",
    body: str = "",
    body_type: Literal["Text", "Html"] = "Html",
    attachments: Iterable[Path | str] | None = None,
):

    assert body_type in ["Text", "Html"], "body_type must be either 'Text' or 'Html'"
    assert isinstance(recipients, list) and all(isinstance(r, str) for r in recipients), "Recipients must be a list of strings"
    assert isinstance(sender, str), "Sender must be a string"

    access_token = get_token()
    # construct email message
    message = {
        "subject": subject,
        "body": {"contentType": body_type, "content": body},
        "toRecipients": [{"emailAddress": {"address": r}} for r in recipients],
    }

    if attachments:
        message["attachments"] = [convert_file_to_graph_attachment(f) for f in attachments]

    # send email
    resp = requests.post(
        url=f"https://graph.microsoft.com/v1.0/users/{sender}/sendMail",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        },
        json={
            "message": message,
            "saveToSentItems": True,
        },
    )
    resp.raise_for_status()
    print(f"Sent email to {recipients} from {sender=!r} with {subject=!r}.")


def get_messages(
    *,
    recipient: str,
    folder: str | Literal["Inbox", "SentItems", "DeletedItems", "Archive"] = "Inbox",
    sender: str | None = None,
    received_from: datetime | None = None,
    received_to: datetime | None = None,
    subject_contains: str | None = None,
    only_unread: bool | None = False,
    top: int | None = 100,
) -> dict | None:

    assert received_to is None or received_to.tzinfo, "received_to must be timezone-aware"
    assert received_from is None or received_from.tzinfo, "received_from must be timezone-aware"

    access_token = get_token()
    url = f"https://graph.microsoft.com/v1.0/users/{recipient}/mailFolders/{folder}/messages"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    params = {
        "$top": str(top),
    }
    if subject_contains:
        params["$search"] = f'"{subject_contains}"'
        headers["ConsistencyLevel"] = "eventual"
    # else:
    #     params["$orderby"] = "receivedDateTime desc"

    # $filter for afsender, ulæst, dato, attachments
    parts = []
    if sender:
        parts.append(f"from/emailAddress/address eq '{sender}'")
    if only_unread:
        parts.append(f"isRead eq {only_unread}")
    if received_from:
        parts.append(f"receivedDateTime ge {received_from.isoformat()}")
    if received_to:
        parts.append(f"receivedDateTime le {received_to.isoformat()}")

    flt = " and ".join(parts)
    if flt:
        params["$filter"] = flt

    resp = requests.get(url, headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()
