from tqdm import tqdm

from nyborg_rpa.utils.auth import get_user_login_info
from nyborg_rpa.utils.os2rollekatalog_client import OS2rollekatalogClient
from nyborg_rpa.utils.pad import dispatch_pad_script
from nyborg_rpa.utils.tunstall_client import TunstallGuiClient


def tes_sync(user: str) -> list[str]:
    """Sync TES users with OS2rollekatalog role assignments."""
    password = get_user_login_info(username=user, program="Windows")["password"]
    tes_client = TunstallGuiClient(user=user, password=password)
    rollekatalog_client = OS2rollekatalogClient(kommune="nyborg")

    # Tunstall
    print("fetching TES users...")
    current_active_user = tes_client.search_user(role="Adgang til Borger", employee_text="Medarbejder")
    all_user = tes_client.search_user(role="Alle")
    all_active_user = tes_client.search_user(role="Adgang til Borger")

    # OS2rollekatalog
    print("fetching TES - Medarbejder user from OS2rollekatalog...")
    roles_details = rollekatalog_client.get_userrole_details(role_name="TES - Medarbejder")
    roles_user_assignments = roles_details["assignments"]

    # Compare lists and find users to add
    add_changes = []

    for user in tqdm(roles_user_assignments, total=len(roles_user_assignments), desc="Find add changes"):
        matches = next(
            (
                u
                for u in current_active_user
                if ((str(u["Brugernavn"]).lower() if "@" in str(u["Brugernavn"]) else f"{str(u['Brugernavn']).lower()}@nyborg.dk") == f"{user['userId'].lower()}@nyborg.dk")
            ),
            None,
        )
        if matches:
            continue
        else:
            matches = next(
                (u for u in all_user if ((str(u["Brugernavn"]).lower() if "@" in str(u["Brugernavn"]) else f"{str(u['Brugernavn']).lower()}@nyborg.dk") == f"{user['userId'].lower()}@nyborg.dk")), None
            )
            if matches:
                check = next(
                    (
                        u
                        for u in all_active_user
                        if ((str(u["Brugernavn"]).lower() if "@" in str(u["Brugernavn"]) else f"{str(u['Brugernavn']).lower()}@nyborg.dk") == f"{user['userId'].lower()}@nyborg.dk")
                    ),
                    None,
                )
                if not check:
                    add_changes.append(f"{matches['Navn']}, {user['userId']}, Tildel")
            else:
                add_changes.append(f"{user['name']}, {user['userId']}, Opret")

    # Compare lists and find users to remove
    remove_changes = []

    for user in tqdm(current_active_user, total=len(current_active_user), desc="Find remove changes"):
        matches = next((u for u in roles_user_assignments if str(u["userId"]).lower() == f"{str(user["Brugernavn"]).replace("@nyborg.dk","")}".lower()), None)
        if matches:
            continue
        else:
            remove_changes.append(f"{user['Navn']}, {str(user["Brugernavn"]).replace("@nyborg.dk","")}, Fjern")

    changes = add_changes + remove_changes

    return changes


if __name__ == "__main__":
    dispatch_pad_script(fn=tes_sync)
