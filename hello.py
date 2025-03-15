import json

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


def select_endpoint(openapi_spec: OpenAPI):
    # TODO: Implement endpoint selection logic
    # For now, return None to indicate no endpoint selected
    return None


def openapi_to_restapi_source(openapi_spec: OpenAPI, endpoint=None):
    # TODO: Implement conversion logic
    # For now, return a dummy source
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
