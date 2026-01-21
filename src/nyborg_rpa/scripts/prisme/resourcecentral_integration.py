import os
from pathlib import Path

import argh
from dotenv import load_dotenv

from nyborg_rpa.utils.email import get_attachments, get_messages, move_message
from nyborg_rpa.utils.pad import dispatch_pad_script


@argh.arg("--recipient", help="mail recipient")
@argh.arg("--sender", help="mail sender")
@argh.arg("--working-dir", help="Path containg data.")
def resourcecentral_integration(*, recipient: str, sender: str, working_dir: Path | str):
    """Fetch emails with attachments from ResourceCentral and save them to Prisme folder."""

    load_dotenv(dotenv_path=r"J:\RPA\.baseflow\.env", override=True)

    working_dir = Path(working_dir)
    prisme_dir = Path(os.environ["PRISME_PATH_RESSOURCE_CENTRAL"])

    print(f"Fetching mails for recipient: {recipient} from sender: {sender}")
    mails = get_messages(recipient=recipient, sender=sender)

    for mail in mails["value"]:

        print(f"Processing mail: {mail["subject"]}")

        attachments = get_attachments(
            recipient=recipient,
            message_id=mail["id"],
            save_to=working_dir,
            ignore_filtype=[".png", ".jpg"],
        )

        for attachment in attachments:

            # read attachment content
            content = attachment.read_text(encoding="utf-8")

            # check if content has data
            if "No data found for the specified date range" in content:
                print(f"No data found in attached file {attachment.name}.")
                attachment.unlink()

            # convert attachment to ANSI and save to Prisme folder
            else:
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
            message_id=mail["id"],
            destination_folder="Archive",
        )


if __name__ == "__main__":
    dispatch_pad_script(fn=resourcecentral_integration)
