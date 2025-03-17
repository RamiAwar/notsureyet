import json
from typing import Any

import dlt
import requests
import yaml
from openapi_pydantic import OpenAPI

spec_url = "https://raw.githubusercontent.com/open-meteo/open-meteo/refs/heads/main/openapi.yml"


def download_spec(url: str) -> dict:
    # Get the file
    with requests.get(url) as response:
        # Check if the request was successful
        response.raise_for_status()

        # Attempt to parse file as json
        try:
            return json.loads(response.text)
        except json.JSONDecodeError:
            # Attempt to parse file as yaml
            try:
                return yaml.safe_load(response.text)
            except yaml.YAMLError as e:
                raise ValueError(f"Failed to parse file as json or yaml: {e}")


def parse_openapi_spec(url: str) -> OpenAPI:
    # Download file into dict, can be json or yaml spec
    spec = download_spec(url)

    # Parse the spec into a pydantic model
    return OpenAPI.model_validate(spec)


def generate_endpoint_descriptions(openapi_spec: OpenAPI) -> dict[str, Any]:
    descriptions = {}
    path_map = openapi_spec.paths or {}
    for path, path_item in path_map.items():
        # Only process GET endpoints
        if path_item.get:
            descriptions[path] = path_item.model_dump(mode="json")

    return descriptions


def select_endpoint(openapi_spec: OpenAPI, user_query: str = ""):
    # TODO: Implement endpoint selection logic
    return None


def select_parameters(openapi_spec: OpenAPI, endpoint: str):
    # TODO: Implement parameter selection logic
    # For now, return None to indicate no parameters selected
    return None


def get_path_server_url(openapi_spec: OpenAPI, path: str) -> str:
    """Get the server URL from the OpenAPI spec.
    Checks for path-level servers first, then falls back to root-level servers.
    Raises ValueError if no server URL is found."""

    # Check path-level servers first
    if path and openapi_spec.paths and path in openapi_spec.paths:
        path_item = openapi_spec.paths[path]
        if path_item.servers and len(path_item.servers) > 0:
            return path_item.servers[0].url

    # Check root-level servers
    if openapi_spec.servers and len(openapi_spec.servers) > 0:
        return openapi_spec.servers[0].url

    # If no servers are defined, raise an error
    raise ValueError("No server url found in openapi spec")


def openapi_to_restapi_source(openapi_spec: OpenAPI, endpoint=None):
    # TODO: Implement conversion logic
    # For now, return a dummy source
    _ = get_path_server_url(openapi_spec)  # Verify server URL exists
    return []


def main():
    # Extract structure from openapi spec
    openapi_spec = parse_openapi_spec(url=spec_url)

    # Select single endpoint (exclude non-GET endpoints)
    endpoint = select_endpoint(openapi_spec)

    source = openapi_to_restapi_source(openapi_spec, endpoint)

    pipeline = dlt.pipeline(pipeline_name="test", destination="duckdb", dataset_name="chat_1_call_1")
    pipeline.run(source)


if __name__ == "__main__":
    main()
