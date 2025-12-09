import os
import warnings
from typing import Literal, TypedDict

import pandas as pd
import pyodbc
from dotenv import load_dotenv


class LoginInfo(TypedDict, total=True):
    username: str
    password: str


def get_auth_table() -> pd.DataFrame:
    """Get the table with the login information for the robots/users from the SQL server."""

    sql_drivers = pyodbc.drivers()
    assert "SQL Server" in sql_drivers, f"SQL Server driver not found in {sql_drivers=}"

    # load environment variables from .env file
    load_dotenv(override=True)

    server = os.getenv("SQL_SERVER")
    database = os.getenv("SQL_DATABASE")
    sym_key = os.getenv("SQL_SYM_KEY")
    cert = os.getenv("SQL_CERT")
    table = os.getenv("SQL_TABLE")

    conn_str = "DRIVER={SQL Server};" f"SERVER={server};" f"DATABASE={database};" "Trusted_Connection=yes;"

    query = (
        f"OPEN SYMMETRIC KEY {sym_key} DECRYPTION BY CERTIFICATE {cert};"
        f"SELECT Navn, Username, Last_Modified, Program, CONVERT(VARCHAR(MAX), DecryptByKey(Password)) AS Password FROM {table};"
        f"CLOSE SYMMETRIC KEY {sym_key};"
    )

    # connect to the SQL server
    conn = pyodbc.connect(conn_str, readonly=True, autocommit=False)

    # execute the SQL query to retrieve the login information
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning, message="pandas only supports SQLAlchemy connectable")
        df = pd.read_sql_query(query, conn)

    conn.close()

    return df


def get_usernames() -> list[str]:
    """Get the list of robot names from the SQL server."""

    # get the auth info from the SQL server
    df = get_auth_table()

    # find the row with the specified name and program
    df = df.query("Program == 'Windows'")

    # check if the result is as expected
    assert len(df) > 0, f"expected len larger than 0, but is {len(df)}"

    # retrieve the list of robot names from the DataFrame
    usernames = sorted(df["Navn"].tolist())

    return usernames


def get_user_login_info(
    *,
    username: str,
    program: Literal["Windows", "Nova"],
) -> LoginInfo:
    """Get the login information for a robot from the SQL server."""

    # get the auth info from the SQL server
    df = get_auth_table()

    # find the row with the specified name and program
    df = df.query(f"Navn == '{username}' and Program == '{program}'")

    # check if the result is as expected
    assert len(df) == 1, f"expected len should be 1, but is {len(df)}"

    # retrieve the username and password from the DataFrame
    user_info = LoginInfo(
        username=df.loc[:, "Username"].item(),
        password=df.loc[:, "Password"].item(),
    )

    return user_info


if __name__ == "__main__":

    # example usage
    usernames = get_usernames()
    print(f"{usernames=}")

    # get login info for a specific robot
    username = input("Enter username: ")
    login_info = get_user_login_info(username=username, program="Windows")
    print(f"{login_info=}")
