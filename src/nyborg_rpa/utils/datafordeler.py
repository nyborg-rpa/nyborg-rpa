import os
import re
from pathlib import Path

import pandas as pd
import requests
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
)
from cryptography.hazmat.primitives.serialization.pkcs12 import (
    load_key_and_certificates,
)
from dotenv import load_dotenv


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


def parse_address(address: dict) -> str:
    """
    Returnerer adresse i format:
    'Vejnavn Husnr[bogstav], [etage] [dør], Postnr Bynavn'
    """

    def parse_street_number(street_number: str) -> str:
        import re

        if street_number.isdigit():
            return str(int(street_number))

        match = re.match(r"^(\d+)([a-zA-Z])$", street_number)
        number_part = int(match.group(1))  # fjerner fx foranstillede nuller
        letter_part = match.group(2)  # beholder evt. bogstaver

        return f"{number_part}{letter_part}"

    def parse_street_floor(floor: str) -> str:
        if floor is None:
            return ""
        if floor.isdigit():
            return f"{int(floor)}."
        return f"{floor}."

    street_details = []

    street_name = address.get("vejadresseringsnavn")
    street_number = parse_street_number(address.get("husnummer"))

    street_details.append(f"{street_name} {street_number}")

    street_floor = parse_street_floor(address.get("etage"))
    street_door = address.get("sidedoer")

    if street_floor and street_door:
        street_details.append(f"{street_floor} {street_door}")
    elif street_floor:
        street_details.append(street_floor)
    elif street_door:
        street_details.append(street_door)

    street_district_number = address.get("postnummer")
    street_district = address.get("postdistrikt")

    street_details.append(f"{street_district_number} {street_district}")

    street = ", ".join(street_details)

    return street


class DatafordelerClient:

    def __init__(self):
        load_dotenv(override=True, verbose=True)
        self.cer_file = Path(os.environ["DATAFORDELER_CER_FILE"])
        self.pfx_file = Path(os.environ["DATAFORDELER_PFX_FILE"])

        assert self.cer_file.exists(), f"Certificate file {self.cer_file} does not exist."
        assert self.pfx_file.exists(), f"PFX file {self.pfx_file} does not exist."

        self.pem_file = pfx_cert_to_pem(
            filepath=self.pfx_file,
            password=os.environ["DATAFORDELER_PASSWORD"],
        )
        self.kommune_kode = "0450"
        self.base_params = {
            "format": "json",
            "pageSize": 200,
        }

    def get(self, url: str, params: dict) -> dict | None:
        resp = requests.get(
            url=url,
            params=(self.base_params | params),
            cert=str(self.pem_file),
            verify=True,
        )
        resp.raise_for_status()
        data = resp.json()

        return data

    def fech_citizens_data(self, data: dict) -> dict:
        citizens = []
        for person in data["Personer"]:
            citizens.append(
                {
                    "cpr": next(person_info["Personnummer"]["personnummer"] for person_info in person["Person"]["Personnumre"] if person_info["Personnummer"]["status"] == "aktuel"),
                    "name": next(f'{navn["Navn"]["fornavne"]} {navn["Navn"]["efternavn"]}' for navn in person["Person"]["Navne"] if navn["Navn"]["status"] == "aktuel"),
                    "address": parse_address(
                        address=next(address["Adresseoplysninger"]["CprAdresse"] for address in person["Person"]["Adresseoplysninger"] if address["Adresseoplysninger"]["CprAdresse"])
                    ),
                    "birthday": pd.to_datetime(person["Person"]["foedselsdato"]).strftime("%d-%m-%Y"),
                    "civil_status": next((civil["Civilstand"]["Civilstandstype"] for civil in person["Person"]["Civilstande"] if civil["Civilstand"]["status"] == "aktuel"), None),
                    "civil_valid_from": pd.to_datetime(
                        next((civil["Civilstand"]["virkningFra"] for civil in person["Person"]["Civilstande"] if civil["Civilstand"]["status"] == "aktuel"), None)
                    ).strftime("%d-%m-%Y"),
                    "partner_cpr": next(
                        (
                            civil["Civilstand"]["Aegtefaelle"]["aegtefaellePersonnummer"]
                            for civil in person["Person"]["Civilstande"]
                            if (civil["Civilstand"]["status"] == "aktuel" and civil["Civilstand"]["Civilstandstype"] == "gift")
                        ),
                        None,
                    ),
                }
            )

        return citizens
