import os
from select import select
from typing import Callable

is_running = True
selected = dict()
tbl_user_info = None  # AgGrid
close_loading_box = lambda: None  # placeholder for the loading box closer

IPC_ADDRESS = ("localhost", 19845)
IPC_AUTHKEY = b"rdp_login_secret"


def loading_splash(text: str, *, timeout: int = None) -> Callable[[], None]:
    """Show a loading splash box using native Windows API. Returns a function to close the box."""

    if os.name != "nt":
        return lambda: None

    import ctypes
    import ctypes.wintypes
    import threading

    WS_POPUP = 0x80000000
    WS_VISIBLE = 0x10000000
    WS_BORDER = 0x00800000
    WS_EX_TOPMOST = 0x00000008
    WS_EX_TOOLWINDOW = 0x00000080
    WM_CLOSE = 0x0010
    WM_QUIT = 0x0012
    SS_CENTER = 0x00000001
    SS_CENTERIMAGE = 0x00000200
    SS_NOPREFIX = 0x00000080

    hwnd, tid = None, None

    def gui():

        nonlocal hwnd, tid

        # create window
        tid = ctypes.windll.kernel32.GetCurrentThreadId()
        hinst = ctypes.windll.kernel32.GetModuleHandleW(None)

        # center on screen
        w, h = 260, 90
        sw, sh = ctypes.windll.user32.GetSystemMetrics(0), ctypes.windll.user32.GetSystemMetrics(1)
        x, y = (sw - w) // 2, (sh - h) // 2

        style = WS_POPUP | WS_VISIBLE | WS_BORDER | SS_CENTER | SS_CENTERIMAGE | SS_NOPREFIX
        hwnd = ctypes.windll.user32.CreateWindowExW(WS_EX_TOPMOST | WS_EX_TOOLWINDOW, "Static", text, style, x, y, w, h, None, None, hinst, None)
        ctypes.windll.user32.UpdateWindow(hwnd)

        msg = ctypes.wintypes.MSG()
        while ctypes.windll.user32.GetMessageW(ctypes.byref(msg), None, 0, 0) > 0:
            ctypes.windll.user32.TranslateMessage(ctypes.byref(msg))
            ctypes.windll.user32.DispatchMessageW(ctypes.byref(msg))

    threading.Thread(target=gui, daemon=True).start()

    def close():
        if hwnd:
            ctypes.windll.user32.PostMessageW(hwnd, WM_CLOSE, 0, 0)  # destroy the window
        if tid:
            ctypes.windll.user32.PostThreadMessageW(tid, WM_QUIT, 0, 0)  # end the message loop

    if timeout:
        threading.Timer(timeout, close).start()

    return close


def try_send_show_command():
    """Try to send a show command to existing instance."""

    from multiprocessing.connection import Client as MPClient

    try:
        print(f"Sending show command to {IPC_ADDRESS}")
        conn = MPClient(IPC_ADDRESS, authkey=IPC_AUTHKEY)
        conn.send("show")
        conn.close()
        return True

    except (ConnectionRefusedError, OSError, Exception):
        return False


def rdp_login_app():

    import threading
    import time
    from multiprocessing.connection import Listener

    from nicegui import Client, app, ui
    from nicegui.events import GenericEventArguments, ValueChangeEventArguments

    from nyborg_rpa.utils.auth import get_auth_table, get_user_login_info
    from nyborg_rpa.utils.rdp import start_windows_rdp

    def ipc_listener_thread():
        """Thread to listen for IPC commands from new instances."""

        # create IPC listener and wait for connections
        listener = Listener(IPC_ADDRESS, authkey=IPC_AUTHKEY)
        while is_running:

            # wait up to 0.1s for a connection
            rlist, _, _ = select([listener._listener._socket], [], [], 0.1)
            if not rlist:
                continue  # nothing yet, loop again

            conn = listener.accept()
            if conn.poll(timeout=0.1):

                msg = conn.recv()
                if msg == "show":
                    app.native.main_window.show()
                    time.sleep(0.1)
                    app.native.main_window.set_always_on_top(True)
                    time.sleep(0.5)
                    app.native.main_window.set_always_on_top(False)
                    app.native.main_window.restore()

            conn.close()

        listener.close()

    def hide_window(event):
        app.native.main_window.minimize()
        app.native.main_window.hide()

    def filter_table(event: GenericEventArguments):
        tbl_user_info.run_grid_method("setFilterModel", {"Navn": {"filterType": "text", "type": "equals", "filter": selected["user"]}})
        tbl_user_info.run_grid_method("onFilterChanged")

    def start_server(event: ValueChangeEventArguments):

        if "server" not in selected or "user" not in selected:
            ui.notify("Please select a server and a user.")
            return

        # get user login info
        user_info = get_user_login_info(username=selected["user"], program="Windows")

        # start RDP on new thread to avoid blocking the UI
        threading.Thread(
            target=start_windows_rdp,
            kwargs={
                "host": selected["server"],
                "username": user_info["username"],
                "password": user_info["password"],
                "fullscreen": True,
            },
            daemon=True,
        ).start()

        # clear selection and hide window
        selected.update({"server": None, "user": None})
        hide_window(event)

    def copy_cell(e: GenericEventArguments):

        value, col_id = e.args.get("value"), e.args.get("colId")
        if col_id in {"Username", "Password"}:
            ui.notify(f"Copied {col_id} to clipboard")
            ui.clipboard.write(value)

    @app.on_disconnect
    def on_disconnect():
        global is_running
        is_running = False

    # start IPC listener that will handle commands from new instances
    threading.Thread(target=ipc_listener_thread, daemon=True).start()

    # load usernames from database
    user_info = get_auth_table().filter(items=["Navn", "Username", "Password", "Program"])
    usernames = user_info.query("Program == 'Windows'").sort_values("Navn")["Navn"].tolist()
    servers = ["NBRPA0", "NBRPA1", "NBRPA2", "NBRPA3", "NBRPAS"]

    @ui.page("/")
    def index(client: Client):

        global tbl_user_info  # TODO: convert app to a class to avoid global

        # close the loading box if still open
        close_loading_box()

        # make content centered
        client.content.classes("h-screen w-screen flex justify-start items-center overflow-hidden")

        # dropdown and buttons
        with ui.row(align_items="baseline"):
            ui.button(icon="filter_alt_off", on_click=lambda e: selected.update({"server": None, "user": None})).bind_enabled_from(
                globals(),
                "selected",
                backward=lambda s: bool(s.get("server") or s.get("user")),
            ).props("flat").classes("self-stretch")
            ui.select(servers, label="Server", with_input=True).bind_value(selected, "server")
            ui.select(usernames, label="User", with_input=True, on_change=filter_table).bind_value(selected, "user")
            ui.button("Start", on_click=start_server)
            ui.button("Hide", on_click=hide_window)
            # ui.button("Exit", on_click=lambda e: app.shutdown(), color="negative")

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
            auto_size_columns=True,
        ).classes("flex-1 h-full")

        # force columns to fit
        tbl_user_info.run_grid_method("sizeColumnsToFit")

        # handler to copy cell value to clipboard on double click
        tbl_user_info.on("cellDoubleClicked", copy_cell)

    # run the app
    ui.run(
        native=True,
        reload=False,
        reconnect_timeout=0,
        title="RDP Login",
        show=True,
        window_size=(800, 600),
    )


def main():
    """Main entry point for the RDP Login application."""

    # cmd.exe /c start "" "powershell.exe" -Command "cd 'C:\nyborg-rpa'; uv run --active rdp_login"
    # cmd.exe /c start "" "powershell.exe" -WindowStyle Hidden -Command "cd 'C:\nyborg-rpa'; uv run --active rdp_login"
    # conhost.exe --headless powershell.exe -NoProfile -WindowStyle Hidden -NonInteractive -Command "cd 'C:\nyborg-rpa'; & uv run --active rdp_login"
    # conhost.exe powershell.exe -NoExit -NoProfile -Command "cd 'C:\nyborg-rpa'; & uv run --active rdp_login"

    global close_loading_box
    close_loading_box = loading_splash("Opening RDP Login...")

    # try to communicate with existing instance
    if try_send_show_command():
        print("Application is already running. Showing existing window.")
        close_loading_box()
        os._exit(0)

    else:
        print("Starting new instance of the application...")
        rdp_login_app()


if __name__ == "__main__":
    main()
