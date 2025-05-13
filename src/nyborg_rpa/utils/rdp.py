import subprocess
import sys


def start_windows_rdp(
    *,
    host: str,
    username: str,
    password: str,
    fullscreen: bool = True,
):
    """
    Start a Windows Remote Desktop Protocol (RDP) session to a remote machine `host` with a given
    `username` and `password` using the `mstsc` command-line tool. Use `win32cred` to cache credentials,
    which are deleted after the connection is closed or times out.

    Args:
        host: The hostname or IP address of the remote machine.
        username: The username to use for the connection.
        password: The password for the user.
        fullscreen: Whether to start the RDP session in fullscreen mode. Defaults to True.
    """

    # check that system is Windows
    if not sys.platform.startswith("win"):
        raise OSError("This function is only supported on Windows.")

    # store credentials in Windows Credential Manager (temporary)
    import win32cred

    win32cred.CredWrite(
        Credential={
            "Type": win32cred.CRED_TYPE_GENERIC,
            "TargetName": f"TERMSRV/{host}",
            "UserName": username,
            "CredentialBlob": password,
            "Persist": win32cred.CRED_PERSIST_SESSION,
        },
        Flags=0,
    )

    # start RDP session
    # clear the credentials after the session is closed or timed out
    args = ["mstsc", f"/v:{host}"]
    if fullscreen:
        args.append("/f")

    try:
        proc = subprocess.Popen(args)
        proc.wait(timeout=10)

    except subprocess.TimeoutExpired:
        pass

    finally:
        win32cred.CredDelete(f"TERMSRV/{host}", win32cred.CRED_TYPE_GENERIC, 0)
