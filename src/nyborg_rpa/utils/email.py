import base64
import getpass
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


def get_messages_in_folder(
    *,
    recipient: str,
    folder: str | Literal["Inbox", "SentItems", "DeletedItems", "Archive"] = "Inbox",
    sender: str | None = None,
    received_from: datetime | None = None,
    received_to: datetime | None = None,
    subject_contains: str | None = None,
    only_unread: bool | None = False,
    top: int | None = 100,
) -> list[dict]:

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

    # build filter param
    filters = []
    if sender:
        filters += [f"from/emailAddress/address eq '{sender}'"]

    if only_unread:
        filters += ["isRead eq true"]

    if received_from:
        filters += [f"receivedDateTime ge {received_from.isoformat()}"]

    if received_to:
        filters += [f"receivedDateTime le {received_to.isoformat()}"]

    # apply filter string
    params["$filter"] = " and ".join(filters)

    # fetch messages
    print(f"Fetching messages for {recipient=!r} in folder {folder=!r} with filters: {params['$filter']!r} and search: {params.get('$search', None)!r}...")
    resp = requests.get(url, headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    messages = data.get("value", [])

    return messages


def get_attachments(
    *,
    recipient: str,
    folder: str | Literal["Inbox", "SentItems", "DeletedItems", "Archive"] = "Inbox",
    message_id: str,
    save_to: str | Path | None = None,
    ignore_filtype: list[str] | None = None,
) -> list[Path]:
    access_token = get_token()
    url = f"https://graph.microsoft.com/v1.0/users/{recipient}/mailFolders/{folder}/messages/{message_id}/attachments"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    attachments = resp.json().get("value", [])

    if save_to:
        save_to = Path(save_to)
    else:
        recipient = getpass.getuser()
        save_to = Path(f"C:/Users/{recipient}/Downloads")

    attachments_list = []
    for att in attachments:
        if ignore_filtype and any(att["name"].endswith(ext) for ext in ignore_filtype):
            continue
        Path(save_to / att["name"]).write_bytes(base64.b64decode(att["contentBytes"]))
        attachments_list.append(Path(save_to / att["name"]))

    return attachments_list


def move_message(*, recipient: str, message_id: str, destination_folder: str | Literal["Inbox", "SentItems", "DeletedItems", "Archive"]) -> dict:
    access_token = get_token()
    url = f"https://graph.microsoft.com/v1.0/users/{recipient}/messages/{message_id}/move"
    json = {f"destinationId": f"{destination_folder}"}
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    resp = requests.post(url, headers=headers, json=json, timeout=30)
    resp.raise_for_status()

    return resp.json()
