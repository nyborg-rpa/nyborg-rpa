from nicegui import Client, app, ui
from nicegui.events import GenericEventArguments, ValueChangeEventArguments

from nyborg_rpa.utils.auth import get_auth_table, get_user_login_info
from nyborg_rpa.utils.rdp import start_windows_rdp

selected = dict()
tbl_user_info = None  # AgGrid


def on_server_select(event: ValueChangeEventArguments):
    selected["server"] = event.value


def on_user_select(event: ValueChangeEventArguments):
    user = event.value
    selected["user"] = user

    # filter table to show only the selected user
    tbl_user_info.run_grid_method("setFilterModel", {"Navn": {"filterType": "text", "type": "equals", "filter": user}})
    tbl_user_info.run_grid_method("onFilterChanged")


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


def copy_cell(e: GenericEventArguments):

    value, col_id = e.args.get("value"), e.args.get("colId")

    if col_id in {"Username", "Password"}:
        ui.notify(f"Copied {col_id} to clipboard")
        ui.clipboard.write(value)


@ui.page("/")
def index(client: Client):

    global tbl_user_info  # TODO: convert app to a class to avoid global variables

    # make content centered
    client.content.classes("h-screen w-screen flex justify-start items-center overflow-hidden")

    # load usernames from database
    user_info = get_auth_table().filter(items=["Navn", "Username", "Password", "Program"])
    usernames = user_info.query("Program == 'Windows'").sort_values("Navn")["Navn"].tolist()
    servers = ["NBRPA0", "NBRPA1", "NBRPA2", "NBRPA3", "NBRPAS"]

    # dropdown and buttons
    with ui.row(align_items="center"):
        ui.select(servers, label="Server", with_input=True, on_change=on_server_select)
        ui.select(usernames, label="User", with_input=True, on_change=on_user_select)
        ui.button("Start", on_click=start_server)

    # table with user info
    cols_defs = [
        {"field": "Navn", "filter": True},
        {"field": "Program", "filter": False},
        {"field": "Username"},
        {"field": "Password", "filter": False, "sortable": False, ":valueFormatter": "params => '•'.repeat(8)"},
    ]

    tbl_user_info = ui.aggrid.from_pandas(
        df=user_info,
        options={"columnDefs": cols_defs},
    ).classes("flex-1 h-full")

    # handler to copy cell value to clipboard on double click
    tbl_user_info.on("cellDoubleClicked", copy_cell)


def main():
    # TODO: make the app run in bg + tray icon
    # https://github.com/zauberzeug/nicegui/discussions/980
    ui.run(native=True, reload=False, reconnect_timeout=0, title="Robot Login")


if __name__ == "__main__":
    main()
