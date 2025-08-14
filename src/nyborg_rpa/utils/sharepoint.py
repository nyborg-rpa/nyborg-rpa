import os

from dotenv import load_dotenv
from office365.graph_client import GraphClient


def get_sharepoint_list_items(*, site: str, list_: str) -> dict:
    # This function should be implemented to fetch the SharePoint element data.

    # load environment variables from .env file
    load_dotenv(override=True)

    # Initialize the SharePoint client with the necessary credentials
    sharepoint_client = GraphClient(tenant=os.getenv("MS_GRAPH_TENANT_ID")).with_client_secret(
        client_id=os.getenv("MS_GRAPH_CLIENT_ID"),
        client_secret=os.getenv("MS_GRAPH_CLIENT_SECRET"),
    )

    # Fetch the SharePoint
    # fmt: off
    items = (
        sharepoint_client
        .sites.get_by_url(site)
        .lists.get_by_name(list_)
        .items.get_all()
        .expand(["fields"])
        .execute_query()
    )  # fmt: on

    items = [item.properties["fields"].properties for item in items]

    return items


def get_sharepoint_item_by_id(*, site: str, list_: str, id_: str) -> dict:
    # This function should be implemented to fetch the SharePoint element data.

    items = get_sharepoint_list_items(
        site=site,
        list_=list_,
    )

    try:
        item = next(item for item in items if item["id"] == id_)
    except StopIteration as e:
        raise ValueError(f"No item found with ID {id_} in list {list_}.") from e

    return item
