from pathlib import Path

from cryptography.hazmat.primitives.serialization import Encoding, NoEncryption, PrivateFormat
from cryptography.hazmat.primitives.serialization.pkcs12 import load_key_and_certificates


def pfx_cert_to_pem(*, filepath: Path | str, password: str) -> Path:
    """Convert a PFX certificate to a PEM file to be used with requests."""

    pfx_bytes = Path(filepath).read_bytes()
    key, cert, chain = load_key_and_certificates(pfx_bytes, password.encode(), None)
    if not key or not cert:
        raise ValueError("PFX is missing a key or certificate")

    pem_parts = [
        cert.public_bytes(Encoding.PEM),
        *(c.public_bytes(Encoding.PEM) for c in (chain or [])),
        key.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()),
    ]

    pem_file = filepath.parent / (filepath.stem + ".pem")
    pem_file.write_bytes(b"".join(pem_parts))

    return pem_file
