import os
from pathlib import Path

import argh
from dotenv import load_dotenv

from nyborg_rpa.utils.email import get_attachments, get_messages_in_folder, move_message
from nyborg_rpa.utils.pad import dispatch_pad_script


@argh.arg("--recipient", help="mail recipient")
@argh.arg("--sender", help="mail sender")
@argh.arg("--working-dir", help="Path containg data.")
def resourcecentral_integration(*, recipient: str, sender: str, working_dir: Path | str):
    """Fetch emails with attachments from ResourceCentral and save them to Prisme folder."""

    load_dotenv(dotenv_path=r"J:\RPA\.baseflow\.env", override=True)

    working_dir = Path(working_dir)
    prisme_dir = Path(os.environ["PRISME_PATH_RESSOURCE_CENTRAL"])

    messages = get_messages_in_folder(
        recipient=recipient,
        folder="Inbox",
        sender=sender,
    )

    for msg in messages:

        print(f"Processing mail: {msg["subject"]}")

        attachments = get_attachments(
            recipient=recipient,
            message_id=msg["id"],
            save_dir=working_dir,
            exclude_filetypes=[".png", ".jpg"],
        )

        for attachment in attachments:

            # read attachment content and check for data
            content = attachment.read_text(encoding="utf-8")
            if "No data found for the specified date range" in content:
                print(f"No data found in attached file {attachment.name}.")
                continue

            # convert attachment to ANSI and save to Prisme folder
            print(f"Saving {attachment.name} to Prisme folder as ANSI (Windows-1252) encoding...")
            prisme_file = prisme_dir / attachment.name
            prisme_file.write_text(
                data=content,
                encoding="cp1252",
                errors="replace",
            )

        # move processed mail to Archive folder
        move_message(
            recipient=recipient,
            message_id=msg["id"],
            destination_folder="Archive",
        )


if __name__ == "__main__":
    dispatch_pad_script(fn=resourcecentral_integration)
