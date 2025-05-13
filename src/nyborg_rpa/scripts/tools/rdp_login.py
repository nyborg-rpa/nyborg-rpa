from nicegui import Client, app, ui
from nicegui.events import ValueChangeEventArguments

from nyborg_rpa.utils.auth import get_user_login_info, get_usernames
from nyborg_rpa.utils.rdp import start_windows_rdp

selected = dict()


def on_server_select(event: ValueChangeEventArguments):
    selected["server"] = event.value


def on_user_select(event: ValueChangeEventArguments):
    selected["user"] = event.value


def start_server(event: ValueChangeEventArguments):

    if "server" not in selected or "user" not in selected:
        ui.notify("Please select a server and a user.")
        return

    user_info = get_user_login_info(username=selected["user"], program="Windows")
    app.native.main_window.hide()

    start_windows_rdp(
        host=selected["server"],
        username=user_info["username"],
        password=user_info["password"],
        fullscreen=True,
    )

    app.shutdown()


@ui.page("/")
def index(client: Client):

    # make content centered
    client.content.classes("h-screen w-screen flex justify-start items-center overflow-hidden")

    # load usernames from database
    usernames = get_usernames()
    servers = ["NBRPA0", "NBRPA1", "NBRPA2", "NBRPA3", "NBRPAS"]

    # add ui elements
    with ui.row(align_items="center"):
        ui.select(servers, label="Server", with_input=True, on_change=on_server_select)
        ui.select(usernames, label="User", with_input=True, on_change=on_user_select)
        ui.button("Start", on_click=start_server)


if __name__ == "__main__":
    ui.run(native=True, reload=False, reconnect_timeout=0, title="Robot Login")
