import os
import re
from pathlib import Path
from typing import NotRequired, TypedDict

import pandas as pd
import requests
from dotenv import load_dotenv

from nyborg_rpa.utils.cryptography import pfx_cert_to_pem


class DatafordelerAddress(TypedDict):
    vejadresseringsnavn: str
    husnummer: str
    postnummer: str
    postdistrikt: str
    etage: NotRequired[str]
    sidedoer: NotRequired[str]
    bynavn: NotRequired[str]


def parse_address(address: DatafordelerAddress) -> str:
    """Parse a Datafordeler address into a single-line string on the form 'Street Name Nr, Floor Door, Zip City'."""

    # https://danmarksadresser.dk/om-adresser/saadan-gengives-en-adresse

    street_name = address["vejadresseringsnavn"]  # TODO: add supplerende bynavn
    street_nr = re.sub(r"^0+", "", address["husnummer"])  # 005C, 063, 003A, etc.
    street = f"{street_name} {street_nr}"

    floor = re.sub(r"^0*(.+)$", r"\1.", address.get("etage", ""))  # "02" -> "2.", "st" -> "st."
    door = address.get("sidedoer", "")  # "tv", "a19", etc.
    floor_and_door = " ".join(p for p in (floor, door) if p)

    city = address["postdistrikt"]  # Ebeltoft, Randers C, etc.
    zip_code = address["postnummer"]
    city_line = f"{zip_code} {city}"

    parts = [street, floor_and_door, city_line]
    line = ", ".join(p for p in parts if p)

    return line


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
