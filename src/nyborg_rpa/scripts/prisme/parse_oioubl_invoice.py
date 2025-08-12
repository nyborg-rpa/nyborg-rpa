import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import TypedDict

import argh

from nyborg_rpa.utils.pad import dispatch_pad_script


class InvoiceItem(TypedDict):
    vare_nr: str
    beskrivelse: str
    vare_beskrivelse: str
    note: str
    antal: float
    enhed: str
    enhedspris: float
    moms: float
    pris: float


class InvoiceMetadata(TypedDict):
    faktura_nr: str
    leverandør: str
    cvr: str
    cpr: str
    forfaldsdato: str
    total_beløb: float
    moms_beløb: float
    antal_enheder_i_alt: int
    ydelse: str
    ydelse_nr: int
    ydelse_tidspunkt: str
    ydelse_lokation: str


@argh.arg("--filepath", help="Path to the XML invoice file.")
def parse_oioubl_invoice(*, filepath: Path | str) -> tuple[InvoiceMetadata, list[InvoiceItem]]:
    """
    Extract metadata and invoice items from an OIOUBL XML invoice file.

    Args:
        filepath: Path to the XML invoice file.

    Returns:
        A tuple (metadata, items) with metadata and a list of invoice items.
    """

    # check if the file exists
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"File {filepath} does not exist.")

    ns = {
        "cbc": "urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2",
        "cac": "urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2",
    }

    tree = ET.parse(source=filepath)
    root = tree.getroot()

    items: list[InvoiceItem] = []
    for line in root.findall(".//cac:InvoiceLine", ns):
        items += [
            {
                "vare_nr": line.find(".//cac:SellersItemIdentification//cbc:ID", ns).text,
                "beskrivelse": line.find(".//cac:Item//cbc:Name", ns).text,
                "vare_beskrivelse": getattr(line.find(".//cac:Item//cbc:Description", ns), "text", ""),
                "note": line.find(".//cbc:Note", ns).text,
                "antal": float(line.find("cbc:InvoicedQuantity", ns).text),
                "enhed": line.find("cbc:InvoicedQuantity", ns).attrib["unitCode"],
                "enhedspris": float(line.find(".//cac:Price//cbc:PriceAmount", ns).text),
                "moms": float(line.find(".//cac:TaxTotal//cbc:TaxAmount", ns).text),
                "pris": float(line.find("cbc:LineExtensionAmount", ns).text),
            }
        ]

    # assumed that a "Varebeskrivelse" is the same for all items
    # on the format:
    # Første Enhed
    # Yderligere oplysninger:  Ydelser: Telefontolkning || Sprog: Ukrainsk ||

    ydelse_info = next(x["note"] for x in items)
    ydelse = re.search(r"Ydelser:\s*([^|]+)", ydelse_info).group(1).strip()
    ydelse_nr = None  # TODO: add ydelsenummer once available

    # the ydelse info is typically encoded in a note on the format
    # "Lokation::Ringvej 3a, 5800 Nyborg.Tidspunkt:03-06-2025 Kl. 10:30-11:30. Kundreference: XXXXXX-XXXX,Navn \n\xa0[Bemærk: ..."
    # however, it can also use "Lokation:" with a single colon
    ydelse_extra_info = root.find(".//cbc:Note", ns).text
    assert ydelse_extra_info, "Could not find extra information in the invoice note."
    ydelse_lokation = re.search(r"Lokation:{1,2}\s*(.+?)\.", ydelse_extra_info).group(1)
    ydelse_tidspunkt = re.search(r"Tidspunkt:{1,2}\s*(\d{2}-\d{2}-\d{4})", ydelse_extra_info).group(1)

    metadata: InvoiceMetadata = {
        "faktura_nr": root.find(".//cbc:ID", ns).text,
        "leverandør": root.find(".//cac:AccountingSupplierParty//cbc:Name", ns).text,
        "cvr": root.find(".//cac:AccountingSupplierParty//cbc:CompanyID", ns).text,
        "cpr": root.find(".//cac:AccountingCustomerParty//cac:Contact//cbc:ID", ns).text.split(",")[0],
        "forfaldsdato": root.find(".//cbc:PaymentDueDate", ns).text,
        "total_beløb": float(root.find(".//cbc:PayableAmount", ns).text),
        "moms_beløb": float(root.find(".//cac:TaxTotal//cbc:TaxAmount", ns).text),
        "antal_enheder_i_alt": sum(int(float(q.text)) for q in root.findall(".//cbc:InvoicedQuantity", ns)),
        "ydelse": ydelse,
        "ydelse_nr": ydelse_nr,
        "ydelse_tidspunkt": ydelse_tidspunkt,
        "ydelse_lokation": ydelse_lokation,
    }

    return metadata, items


if __name__ == "__main__":
    dispatch_pad_script(fn=parse_oioubl_invoice)
