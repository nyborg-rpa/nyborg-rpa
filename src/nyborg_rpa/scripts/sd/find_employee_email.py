from nyborg_rpa.utils.os2sofd_client import OS2sofdClient
from nyborg_rpa.utils.pad import dispatch_pad_script


def find_employee_email(*, cpr: str) -> str | None:
    """
    Find the email address of an employee based on their CPR number.

    Args:
        cpr: The CPR number of the employee.

    Returns:
        str: The email address of the employee if found, otherwise None.
    """
    sofd_client = OS2sofdClient(kommune="nyborg")
    user_info = sofd_client.get_user_by_cpr(cpr=cpr)

    email = next((user["UserId"] for user in user_info["Users"] if "@" in user["UserId"]), None)
    if email is None:
        email = next((user["UserId"] for user in user_info["DisabledUsers"] if "@" in user["UserId"]), None)

    return email


if __name__ == "__main__":
    dispatch_pad_script(fn=find_employee_email)

    # Example usage
    # email = find_employee_email(cpr="1234567890")
    # print(email)
