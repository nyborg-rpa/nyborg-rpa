from typing import Literal

from authlib.integrations.httpx_client import OAuth2Client

from nyborg_rpa.utils.auth import get_user_login_info


class NexusClient(OAuth2Client):

    def __init__(self, enviroment: Literal["nexus", "nexus-review"]):

        instance = "nyborg"
        program = {"nexus": "Nexus-Drift", "nexus-review": "Nexus-Test"}[enviroment]
        nexus_info = get_user_login_info(username="API", program=program)

        super().__init__(
            client_id=nexus_info["username"],
            client_secret=nexus_info["password"],
            token_endpoint=f"https://iam.{enviroment}.kmd.dk/authx/realms/{instance}/protocol/openid-connect/token",
            timeout=30.0,
            base_url=f"https://{instance}.{enviroment}.kmd.dk/api/core/mobile/{instance}/v2/",
        )

        # Automatically fetch the token during initialization
        self.fetch_token()


if __name__ == "__main__":
    nexus_client = NexusClient(enviroment="nexus-review")
    print(nexus_client.get("patient/1/preferences/").json())
