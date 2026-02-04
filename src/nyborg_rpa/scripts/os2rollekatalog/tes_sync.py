import os
import re

from bidict import bidict
from frozendict import deepfreeze, frozendict
from tqdm import tqdm

from nyborg_rpa.utils.auth import get_user_login_info
from nyborg_rpa.utils.os2rollekatalog_client import OS2rollekatalogClient
from nyborg_rpa.utils.pad import dispatch_pad_script
from nyborg_rpa.utils.tunstall_client import TunstallGuiClient


def tes_sync() -> list[str]:
    """Sync TES users with OS2rollekatalog role assignments."""

    username = os.environ["USERNAME"]
    password = get_user_login_info(username=username, program="Windows")["password"]
    tes_client = TunstallGuiClient(user=username, password=password)
    rollekatalog_client = OS2rollekatalogClient(kommune="nyborg")

    sofd_role = "TES - Medarbejder"
    tes_role = "Adgang til Borger"
    tes_changes: list[dict[str, str]] = []

    print("Fetching all TES users...")
    tes_users: tuple[frozendict] = deepfreeze(tes_client.search_user(role="Alle"))

    print("Fetching assigned TES users...")
    tes_users_assigned: tuple[frozendict] = deepfreeze(tes_client.search_user(role=tes_role))

    print("Fetching SOFD users...")
    sofd_users: tuple[frozendict] = deepfreeze(rollekatalog_client.get_userrole_details(role_name=sofd_role).get("assignments", []))

    # map SOFD users to TES users
    sofd_tes_user_id_mapping = bidict()
    for sofd_user in sofd_users:
        if tes_user := next((u for u in tes_users if re.sub(r"@.*$", "", u["Brugernavn"]).lower() == sofd_user["userId"].lower()), None):
            sofd_tes_user_id_mapping[sofd_user] = tes_user

    # find users to add to TES
    for sofd_user in tqdm(sofd_users, desc="Finding users to add"):

        tes_user = sofd_tes_user_id_mapping.get(sofd_user)
        is_assigned = tes_user in tes_users_assigned

        if not tes_user:
            tes_changes += [{"name": sofd_user["name"], "user": sofd_user["userId"], "action": "create"}]

        elif not is_assigned:
            tes_changes += [{"name": sofd_user["name"], "user": sofd_user["userId"], "action": "add"}]

    # find users to remove from TES
    for tes_user in tqdm(tes_users_assigned, desc="Finding users to remove"):

        sofd_user = sofd_tes_user_id_mapping.inv.get(tes_user)
        if not sofd_user:
            tes_changes += [{"name": tes_user["Navn"], "user": re.sub(r"@.*$", "", tes_user["Brugernavn"]), "action": "remove"}]

    # filter users (temporary fix)
    tes_changes = [
        change for change in tes_changes
        if change["action"] != "remove"
        or change["user"].startswith("vik")
        or change["user"].startswith("idc")
        or re.match(r"^v\d+", change["user"])  #  users like v12345abc
        or "vikar" in change["user"].lower()
    ]  # fmt: skip

    # convert tes_changes Power Automate Desktop friendly format
    # i.e. "Navn,UserId,Opret|Tildel|Fjern"
    action_map = {
        "create": "Opret",
        "add": "Tildel",
        "remove": "Fjern",
    }
    changes_pad_friendly = []
    for change in tes_changes:
        if change["action"] in action_map:
            changes_pad_friendly += [f"{change["name"]},{change["user"]},{action_map[change["action"]]}"]

    return changes_pad_friendly


if __name__ == "__main__":
    dispatch_pad_script(fn=tes_sync)
