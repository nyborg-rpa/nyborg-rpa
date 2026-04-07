import datetime as dt
import uuid as uuid
from pathlib import Path
from typing import Any

import httpx
from authlib.jose import jwt
from Crypto.PublicKey import ECC


class ABMAuth(httpx.Auth):
    def __init__(
        self,
        *,
        client_id: str,
        key_id: str,
        private_key_pem: str,
    ) -> None:

        self.client_id = client_id
        self.key_id = key_id
        self.private_key = ECC.import_key(private_key_pem)
        self.access_token: str | None = None
        self.token_endpoint = "https://account.apple.com/auth/oauth2/v2/token"

    def update_tokens(self) -> None:

        now = int(dt.datetime.now(tz=dt.timezone.utc).timestamp())
        client_assertion = jwt.encode(
            header={"alg": "ES256", "kid": self.key_id},
            key=self.private_key.export_key(format="PEM"),
            payload={
                "sub": self.client_id,
                "aud": self.token_endpoint,
                "iat": now,
                "exp": now + 300,
                "jti": str(uuid.uuid4()),
                "iss": self.client_id,
            },
        )

        if isinstance(client_assertion, (bytes, bytearray)):
            client_assertion = client_assertion.decode()

        resp = httpx.post(
            url=self.token_endpoint,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_assertion": client_assertion,
                "client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
                "scope": "business.api",
            },
        )
        resp.raise_for_status()
        data: dict[str, Any] = resp.json()

        token = data.get("access_token")
        if not token:
            raise RuntimeError(f"Missing access_token in token response: {data}")

        self.access_token = token

    def auth_flow(self, request: httpx.Request):

        if not self.access_token:
            self.update_tokens()

        request.headers["Authorization"] = f"Bearer {self.access_token}"
        response: httpx.Response = yield request

        if response.status_code == 401:
            response.close()
            self.update_tokens()
            request.headers["Authorization"] = f"Bearer {self.access_token}"

            yield request


class AppleBusinessManagerClient(httpx.Client):

    def __init__(
        self,
        *,
        client_id: str,
        key_id: str,
        private_key_file: Path | str,
        timeout: float = 30.0,
        **kwargs,
    ) -> None:

        private_key_pem = Path(private_key_file).read_text(encoding="utf-8")

        super().__init__(
            base_url="https://api-business.apple.com",
            timeout=timeout,
            auth=ABMAuth(
                client_id=client_id,
                key_id=key_id,
                private_key_pem=private_key_pem,
            ),
            **kwargs,
        )
