import json
import os
import re
from itertools import count
from pathlib import Path
from typing import NotRequired, TypedDict

import httpx
import pandas as pd
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


class DatafordelerClient(httpx.Client):
    """Client for the Danish Datafordeler API."""

    def __init__(
        self,
        *,
        pfx_file: Path | str = None,
        pfx_password: str = None,
        **kwargs,
    ) -> None:

        # load and resolve certificate
        if not pfx_file or not pfx_password:
            load_dotenv(override=True, verbose=True)

        pfx_file = Path(pfx_file or os.environ["DATAFORDELER_PFX_FILE"])
        pfx_password = pfx_password or os.environ["DATAFORDELER_PFX_PASSWORD"]
        assert pfx_file.exists(), f"PFX file {pfx_file.as_posix()!r} does not exist."
        pem_file = pfx_cert_to_pem(filepath=pfx_file, password=pfx_password)

        # initialize client with cert and default params
        super().__init__(
            cert=str(pem_file),
            params={"format": "json", "pageSize": 200},
            verify=True,
            **kwargs,
        )

    def get_persons(
        self,
        params: dict,
        *,
        historical: bool = False,
    ) -> list[dict]:
        """Get persons from Datafordeler CPR endpoint for given query params."""

        if "page" in params:
            raise ValueError("Pagination is handled internally, do not pass 'page' in params.")

        if historical:
            raise NotImplementedError("Historical CPR endpoint not implemented yet.")

        url = "https://s5-certservices.datafordeler.dk/CPR/CPRPersonFullComplete/1/REST/PersonFullCurrentListComplete"
        page_size = params.get("pageSize", 500)
        params |= {"pageSize": page_size}

        persons = []
        for page in count(1):

            resp = self.get(
                url=url,
                params=params | {"page": page},
            )

            resp.raise_for_status()
            data = resp.json()  # {"Personer": [{"Person": {...}, ...}]}
            new_persons = [p["Person"] for p in data["Personer"]]
            persons += new_persons

            if not new_persons or len(new_persons) < page_size:
                break  # no more pages

        if not historical:
            persons = [prune_historical_records(p) for p in persons]

        return persons

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
                    "birthday": pd.to_datetime(str(person["Person"]["foedselsdato"])).to_pydatetime().date(),
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


def prune_historical_records(obj: dict) -> dict:
    """
    Remove list elements with `status == "historisk"` from a Datafordeler object.

    A Datafordeler object is structured as: `{id: ..., Navne: [{Navn: {status: "historisk|aktuel", ...}, ...]}`
    """

    def is_historical(entry: dict) -> bool:
        return '"status": "historisk"' in json.dumps(entry, ensure_ascii=False)

    def prune_list(entries: list[dict]) -> list[dict]:
        return [e for e in entries if not is_historical(e)]

    pruned = {}
    for key, value in obj.items():
        if isinstance(value, list):
            pruned[key] = prune_list(value)
        else:
            pruned[key] = value

    return pruned
