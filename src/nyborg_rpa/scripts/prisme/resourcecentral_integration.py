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


@argh.arg("--recipient", help="mail recipient")
@argh.arg("--sender", help="mail sender")
@argh.arg("--working-dir", help="Path containg data.")
def resourcecentral_integration(*, recipient: str, sender: str, working_dir: Path | str):
    """Fetch emails with attachments from ResourceCentral and save them to Prisme folder."""
    load_dotenv(dotenv_path=r"J:\RPA\.baseflow\.env", override=True)

    working_dir: Path = Path(working_dir)
    prisme_dir: Path = Path(os.environ["PRISME_PATH_RESSOURCE_CENTRAL"])
    print(f"Fetching mails for recipient: {recipient} from sender: {sender}")
    mails = get_messages(recipient=recipient, sender=sender)

    for mail in mails["value"]:
        print(f"Processing mail: {mail['subject']}")

        attachments = get_attachments(recipient=recipient, message_id=mail["id"], save_to=working_dir, ignore_filtype=[".png", ".jpg"])
        for file_path in attachments:
            # check content have data
            with open(file_path, encoding="utf-8") as f:
                content = f.read()

            if "No data found for the specified date range" in content:
                print(f"--No data found in attached file {Path(file_path).name}.")
                os.remove(file_path)

            # Convert UTF-8 file to ANSI and save to prisme folder
            else:
                print(f"--Saving {Path(file_path).name} to Prisme folder.")
                output_file = prisme_dir / Path(file_path).name
                # output_file = working_dir / Path(file_path).name
                convert_utf8_to_ansi(file_path, output_file)

        move_message(recipient=recipient, message_id=mail["id"], destination_folder="Archive")


if __name__ == "__main__":
    dispatch_pad_script(fn=resourcecentral_integration)

    # Test
    # resourcecentral_integration(recipient="", sender="", working_dir=r"")
