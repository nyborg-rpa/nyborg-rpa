from nyborg_rpa.utils.auth import get_user_login_info
from nyborg_rpa.utils.nexus_client import NexusClient
from nyborg_rpa.utils.pad import dispatch_pad_script


def dynamic_temp_send_letter(*, letter_uuid: str):

    login_info = get_user_login_info(
        username="API",
        program="Nexus-Drift",
    )

    nexus_client = NexusClient(
        client_id=login_info["username"],
        client_secret=login_info["password"],
        instance="nyborg",
        enviroment="nexus",
    )

    # fetch letter
    link = f"letters/withAttachment?uid={letter_uuid}"
    resp = nexus_client.get(link)
    data = resp.json()

    # send letter
    send_letter_link = data["_links"]["updateAndSendExternally"]["href"]
    resp = nexus_client.put(send_letter_link, json=data)
    resp.raise_for_status()


if __name__ == "__main__":
    dispatch_pad_script(fn=dynamic_temp_send_letter)
    # dynamic_temp_send_letter(letter_uuid="")
