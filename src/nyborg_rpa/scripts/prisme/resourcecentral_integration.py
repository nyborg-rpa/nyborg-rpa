import os
from pathlib import Path

import argh
from dotenv import load_dotenv

from nyborg_rpa.utils.email import get_attachments, get_messages, move_message
from nyborg_rpa.utils.pad import dispatch_pad_script


def convert_utf8_to_ansi(input_file: Path | str, output_file: Path | str):
    # Læs filen som UTF-8
    with open(input_file, "r", encoding="utf-8") as f:
        content = f.read()

    # Skriv filen som Windows-1252 (ANSI)
    with open(output_file, "w", encoding="cp1252", errors="replace") as f:
        f.write(content)
@argh.arg("--sender", help="mail sender")
def resourcecentral_integration(*, recipient: str, sender: str):
    """Fetch emails with attachments from ResourceCentral and save them to Prisme folder."""
    load_dotenv(override=True)

    prisme_dir: Path = Path(os.environ["PRISME_PATH_RESSOURCE_CENTRAL"])
    mails = get_messages(recipient=recipient, sender=sender)

    for mail in mails["value"]:
        get_attachments(recipient=recipient, message_id=mail["id"], save_to=prisme_dir, ignore_filtype=[".png", ".jpg"])
        move_message(recipient=recipient, message_id=mail["id"], destination_folder="Archive")


if __name__ == "__main__":
    dispatch_pad_script(fn=resourcecentral_integration)

    # Test
    # from_mail = ""
    # to_mail = ""
    # resourcecentral_integration(recipient=to_mail, sender=from_mail, working_dir="")
