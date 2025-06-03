import argh

from nyborg_rpa.utils.pad import dispatch_pad_script

NEXUS_INSTANCE = "nyborg"
NEXUS_TOKEN_URL = f"https://iam.nexus.kmd.dk/authx/realms/{NEXUS_INSTANCE}/protocol/openid-connect/token"
NEXUS_BASE_URL = f"https://{NEXUS_INSTANCE}.nexus.kmd.dk/api/core/mobile/{NEXUS_INSTANCE}/v2/"


def init_client():

    from authlib.integrations.httpx_client import OAuth2Client

    from nyborg_rpa.utils.auth import get_user_login_info

    global client

    nexus_info = get_user_login_info(username="API", program="Nexus-Drift")
    client_id = nexus_info["username"]
    client_secret = nexus_info["password"]

    # Set up the OAuth2 client
    client = OAuth2Client(
        client_id=client_id,
        client_secret=client_secret,
        token_endpoint=NEXUS_TOKEN_URL,
        timeout=30.0,
    )

    # Automatically fetch the token during initialization
    token = client.fetch_token()


# def close_item(link: str):
def close_item(item: dict):

    document_url = item["_links"]["self"]["href"]
    document_name = item["name"]
    print(f"Closing item: '{document_name}'...")
    document = client.get(document_url).json()

    ref_obj_url = document["_links"]["referenceObject"]["href"]
    ref_obj = client.get(ref_obj_url).json()

    available_actions_url = ref_obj["_links"].get("availableActions", {}).get("href")
    if not available_actions_url:
        print(f"No available actions found for: {document_name}")
        return

    available_actions = client.get(available_actions_url).json()
    available_actions_names = {action["name"] for action in available_actions}

    if "Inaktivt" in available_actions_names and "Låst" in available_actions_names:
        raise ValueError(f"Both 'Inaktivt' and 'Låst' actions are available for: {document_name}")

    update_form_url = next(
        a["_links"]["updateFormData"]["href"] for a in available_actions if a["name"] in {"Inaktivt", "Låst"}
    )
    update_form_body = client.get(update_form_url).json()

    defaults = {
        "Betydning for situation/borgerens tilstand": "Uændret",
    }

    missing_items = [item for item in update_form_body["items"] if item.get("required") and item["value"] is None]
    if missing_items:

        labels = [item["label"] for item in missing_items]
        print(f"Item {document_name} has missing required items: {labels}")

        for missing_item in missing_items:

            label = missing_item["label"]
            if label not in defaults:
                raise ValueError(f"Missing default value for required item: {label}")

            possible_value = next((pv for pv in missing_item["possibleValues"] if pv["name"] == defaults[label]), None)
            if possible_value is None:
                print(f"Skipping item '{label}' as default value '{defaults[label]}' not found in possible values.")
                return

            # set the value to the default
            print(f"Setting default value for '{label}': {defaults[label]}")
            update_form_body["items"][update_form_body["items"].index(missing_item)]["value"] = possible_value

    resp = client.put(url=update_form_url, json=update_form_body)
    resp.raise_for_status()


def close_all_items(item: dict):

    if item.get("type") == "formDataV2Reference":
        close_item(item)

    elif len(item["children"]) == 0:
        return

    else:
        for child in item["children"]:
            close_all_items(child)


@argh.arg("--patient-id", help="The Nexus patient ID to close documents for.", type=int)
def close_patient_documents(*, patient_id: int):
    """
    Close all documents for a given Nexus patient by setting them to 'Inaktivt' or 'Låst'.

    Args:
        patient_id: The Nexus patient ID to close documents for.
    """

    init_client()

    resp = client.get(f"{NEXUS_BASE_URL}/patient/{patient_id}/preferences/")
    preferences = resp.json()

    close_schema_link = None
    for pathway in preferences["CITIZEN_PATHWAY"]:
        if pathway["name"] == "Robot - Luk skema":
            close_schema_link = pathway["_links"]["self"]["href"]
            break

    if close_schema_link is None:
        raise ValueError("Could not find 'Robot - Luk skema' in patient preferences.")

    resp = client.get(close_schema_link)
    pathway_data = resp.json()
    references_link = pathway_data["_links"]["pathwayReferences"]["href"]
    references_data = client.get(references_link).json()

    if "patientActivities" in pathway_data["_links"]:
        raise ValueError("Patient activities link found, but not implemented.")

    close_all_items(item={"children": references_data})


if __name__ == "__main__":
    dispatch_pad_script(fn=close_patient_documents)
