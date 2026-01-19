import argh

from nyborg_rpa.utils.auth import get_user_login_info
from nyborg_rpa.utils.nexus_client import NexusClient
from nyborg_rpa.utils.pad import dispatch_pad_script

nexus_client: NexusClient


# def close_item(link: str):
def close_item(item: dict):

    global nexus_client

    document_name = item["formDefinition"]["title"]
    print(f"Closing item: '{document_name}'...")

    available_actions_url = item["_links"]["availableActions"]["href"]

    if not available_actions_url:
        print(f"No available actions found for: {document_name}")
        return

    available_actions = nexus_client.get(available_actions_url).json()
    available_actions_names = {action["name"] for action in available_actions}

    if "Inaktivt" in available_actions_names and "Låst" in available_actions_names:
        raise ValueError(f"Both 'Inaktivt' and 'Låst' actions are available for: {document_name}")

    update_form_url = next(a["_links"]["updateFormData"]["href"] for a in available_actions if a["name"] in {"Inaktivt", "Låst"})
    update_form_body = nexus_client.get(update_form_url).json()

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
                print(f"Skipping item '{label}' as it has no default value.")
                return

            possible_value = next((pv for pv in missing_item["possibleValues"] if pv["name"] == defaults[label]), None)
            if possible_value is None:
                print(f"Skipping item '{label}' as default value '{defaults[label]}' not contain possible value.")
                return

            # set the value to the default
            print(f"Setting default value for '{label}': {defaults[label]}")
            update_form_body["items"][update_form_body["items"].index(missing_item)]["value"] = possible_value

    resp = nexus_client.put(url=update_form_url, json=update_form_body)
    resp.raise_for_status()


@argh.arg("--patient-id", help="The Nexus patient ID to close documents for.", type=int)
def close_patient_documents(*, patient_id: int):
    """
    Close all documents for a given Nexus patient by setting them to 'Inaktivt' or 'Låst'.

    Args:
        patient_id: The Nexus patient ID to close documents for.
    """

    global nexus_client

    # initialize Nexus client
    login_info = get_user_login_info(
        username="API",
        program="Nexus-Drift",
    )

    nexus_environment = "nexus"
    nexus_client = NexusClient(
        client_id=login_info["username"],
        client_secret=login_info["password"],
        instance="nyborg",
        enviroment=nexus_environment,
    )

    resp = nexus_client.get(f"/patient/{patient_id}/preferences/")
    preferences = resp.json()

    close_schema_link = None
    for pathway in preferences["CITIZEN_PATHWAY"]:
        if pathway["name"] == "Robot - Luk skema":
            close_schema_link = pathway["_links"]["self"]["href"]
            break

    if close_schema_link is None:
        raise ValueError("Could not find 'Robot - Luk skema' in patient preferences.")

    resp = nexus_client.get(close_schema_link)
    pathway_data = resp.json()

    # patient list is a filter of borgerforløb which contains patient activities and nested pathways
    # where a "pathway" is just a directory which can contain more pathways or formDataV2References (activities)
    activities = []

    # add top-level patient activities
    if "patientActivities" in pathway_data["_links"]:
        patient_activities_link = pathway_data["_links"]["patientActivities"]["href"]
        patient_activities_data = nexus_client.get(patient_activities_link).json()
        for item in patient_activities_data:
            activity_object = nexus_client.get(item["_links"]["self"]["href"]).json()
            activities += [activity_object]

    # add nested activities using DFS
    references_link = pathway_data["_links"]["pathwayReferences"]["href"]
    references_data = nexus_client.get(references_link).json()
    stack: list[dict] = references_data.copy()
    while stack:
        item = stack.pop()
        if item["type"] == "patientPathwayReference":
            stack.extend(item["children"])

        if item["type"] == "formDataV2Reference":
            # Get referenceObject activity
            activity_self = nexus_client.get(item["_links"]["self"]["href"]).json()
            activity_object = nexus_client.get(activity_self["_links"]["referenceObject"]["href"]).json()
            activities += [activity_object]

    # close all activities
    for item in activities:
        if "Kontakter" in item["formDefinition"]["title"]:
            continue
        close_item(item)


if __name__ == "__main__":
    dispatch_pad_script(fn=close_patient_documents)
    # close_patient_documents(patient_id=1)
