import os
import re

from tqdm import tqdm

from nyborg_rpa.utils.auth import get_user_login_info
from nyborg_rpa.utils.os2rollekatalog_client import OS2rollekatalogClient
from nyborg_rpa.utils.pad import dispatch_pad_script
from nyborg_rpa.utils.tunstall_client import TunstallGuiClient


def tes_sync(*, user: str | None = None) -> list[str]:
    """Sync TES users with OS2rollekatalog role assignments."""

    user = user or os.environ["USERNAME"]
    password = get_user_login_info(username=user, program="Windows")["password"]
    tes_client = TunstallGuiClient(user=user, password=password)
    rollekatalog_client = OS2rollekatalogClient(kommune="nyborg")

    # TODO: make TES client return ALL info about a user, e.g. search_users(..., detailed=True) which calls get_user(id)
    print("fetching TES users...")
    current_active_user = tes_client.search_user(role="Adgang til Borger", employee_text="Medarbejder")
    all_user = tes_client.search_user(role="Alle")
    all_active_user = tes_client.search_user(role="Adgang til Borger")

    print("fetching TES - Medarbejder user from OS2rollekatalog...")
    roles_details = rollekatalog_client.get_userrole_details(role_name="TES - Medarbejder")
    roles_user_assignments = roles_details["assignments"]

    print("fetching all users and their role assignments from OS2rollekatalog...")
    users = {}
    roles = rollekatalog_client.get("read/userroles").json()
    for role in tqdm(roles):
        resp = rollekatalog_client.get(f"read/assigned/{role["id"]}", params={"indirectRoles": "true"})
        resp.raise_for_status()
        assigned = resp.json()
        for a in assigned.get("assignments", []):
            u = users.setdefault(a["uuid"], {"uuid": a["uuid"], "userId": a.get("userId"), "name": a.get("name"), "roles": []})
            u["roles"] += [role] if role not in u["roles"] else []

    # compare lists and find users to add
    add_changes = []
    for user in tqdm(roles_user_assignments, desc="Find add changes"):

        tes_user = next((u for u in all_user if re.sub(r"@.*", "", u["Brugernavn"]).lower() == user["userId"].lower()), None)
        is_active = tes_user and tes_user["Brugernavn"] in [u["Brugernavn"] for u in all_active_user]

        # user already exists and is active
        if is_active:
            continue

        # exists in TES but is not active
        if tes_user:
            add_changes += [f"{tes_user["Navn"]}, {user["userId"]}, Tildel"]

        # does not exist in TES
        else:
            add_changes += [f"{user["name"]}, {user["userId"]}, Opret"]

    # compare lists and find users to remove
    remove_changes = []
    for user in tqdm(current_active_user, desc="Find remove changes"):

        should_remove = False
        tes_username = re.sub(r"@.*", "", user["Brugernavn"]).lower()
        rollekatalog_user = next((u for u in users.values() if u["userId"].lower() == tes_username), {})

        if not rollekatalog_user:
            should_remove = True

        systems = {r["itSystemName"] for r in rollekatalog_user.get("roles", [])}
        if not any(s in systems for s in ["Tunstall TES", "Tunstall Service Provider"]):
            should_remove = True

        if should_remove:
            remove_changes += [f"{user["Navn"]}, {tes_username}, Fjern"]

    # NOTE: temporary disabled removal of users
    changes = add_changes
    # changes = add_changes + remove_changes

    return changes


if __name__ == "__main__":
    dispatch_pad_script(fn=tes_sync)
