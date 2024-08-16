import time
import pickle
import xml.etree.ElementTree as ET
from urllib.request import urlopen
from urllib.error import HTTPError
from pathlib import Path


TOPOLOGY_RESOURCE_DATA_URL = "https://topology.opensciencegrid.org/rgsummary/xml"


def get_topology_resource_data(
        force_update=False,
        resource_data_path=Path("binaries/topology_resource_data_map.pickle"),
    ) -> dict:
    """Generates a dictionary that maps (lowercased) resource names
    to institutional information as provided in OSG Topology."""

    # Only fetch Topology data if the Pickle is stale (>1 day old) or missing
    if (
        not force_update and
        resource_data_path.is_file() and
        resource_data_path.stat().st_mtime > (time.time() - 24*3600)
    ):
        resources_map = pickle.load(resource_data_path.open("rb"))

    # Try five times to fetch the data, sometimes Topology is unresponsive
    else:
        tries = 0
        max_tries = 5
        while tries < max_tries:
            try:
                with urlopen(TOPOLOGY_RESOURCE_DATA_URL) as xml:
                    xmltree = ET.parse(xml)
            except HTTPError:
                time.sleep(2**tries)
                tries += 1
                if tries == max_tries:
                    raise
            else:
                break
        resource_groups = xmltree.getroot()

        # Initialize the map with unknown values        
        resources_map = {
            "UNKNOWN": {
                "name": "UNKNOWN",
                "institution": "UNKNOWN",
            }
        }

        # Map the ResourceName to the Institution, Institution ID, etc.
        for resource_group in resource_groups:
            resource_institution = resource_group.find("Facility").find("Name").text
            resource_institution_id = resource_group.find("Facility").find("ID").text

            resources = resource_group.find("Resources")
            for resource in resources:
                resource_map = {}
                resource_map["institution"] = resource_institution
                resource_map["institution_id"] = resource_institution_id
                resource_map["name"] = resource.find("Name").text
                resource_map["id"] = resource.find("ID").text
                resources_map[resource_map["name"].lower()] = resource_map.copy()

        # Save a copy of the map to disk so we don't have to continuously hit Topology
        pickle.dump(resources_map, resource_data_path.open("wb"))

    return resources_map