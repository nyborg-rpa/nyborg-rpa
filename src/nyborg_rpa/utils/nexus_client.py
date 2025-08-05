from typing import Literal

from authlib.integrations.httpx_client import OAuth2Client


class NexusClient(OAuth2Client):

    def __init__(
        self,
        *,
        client_id: str,
        client_secret: str,
        instance: str,
        enviroment: Literal["nexus", "nexus-review"],
    ):
        """
        Initialize the NexusClient with OAuth2 credentials and base URL.

        Args:
            client_id: The client ID for OAuth2 authentication.
            client_secret: The client secret for OAuth2 authentication.
            instance: The `https://{instance}.{enviroment}.kmd.dk` instance.
            enviroment: The `https://{instance}.{enviroment}.kmd.dk` environment.
        """

        super().__init__(
            client_id=client_id,
            client_secret=client_secret,
            token_endpoint=f"https://iam.{enviroment}.kmd.dk/authx/realms/{instance}/protocol/openid-connect/token",
            timeout=30.0,
            base_url=f"https://{instance}.{enviroment}.kmd.dk/api/core/mobile/{instance}/v2/",
        )

        # Automatically fetch the token during initialization
        self.fetch_token()


if __name__ == "__main__":

    from nyborg_rpa.utils.auth import get_user_login_info

    login_info = get_user_login_info(
        username="API",
        program="Nexus-Test",
    )

    nexus_client = NexusClient(
        client_id=login_info["username"],
        client_secret=login_info["password"],
        instance="nyborg",
        enviroment="nexus-review",
    )

    print(nexus_client.get("patient/1/preferences/").json())
