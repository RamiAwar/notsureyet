import json
import os
from dataclasses import dataclass
from typing import Any

import dlt
import logfire
import requests
import yaml
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig
from openapi_pydantic.v3.v3_0 import OpenAPI as OpenAPI30
from openapi_pydantic.v3.v3_0 import Parameter as ParameterV30
from openapi_pydantic.v3.v3_1 import OpenAPI as OpenAPI31
from openapi_pydantic.v3.v3_1 import Parameter as ParameterV31
from pydantic import BaseModel
from pydantic_ai import Agent, ModelRetry, RunContext
from pydantic_ai.format_as_xml import format_as_xml

# Type alias for OpenAPI 3.0 and 3.1
OpenAPI = OpenAPI30 | OpenAPI31

logfire.configure(token=os.getenv("LOGFIRE_TOKEN"))
logfire.instrument_openai()

logfire.info("Hello, {place}!", place="World")


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

    # Determine OpenAPI version
    version = spec.get("openapi", "")
    if version.startswith("3.1"):
        return OpenAPI31.model_validate(spec)
    elif version.startswith("3.0"):
        return OpenAPI30.model_validate(spec)
    else:
        raise ValueError(f"Unsupported OpenAPI version: {version}")


def generate_endpoint_descriptions(openapi_spec: OpenAPI) -> dict[str, Any]:
    descriptions = {}
    path_map = openapi_spec.paths or {}
    for path, path_item in path_map.items():
        # Only process GET endpoints
        if path_item.get:
            descriptions[path] = path_item.model_dump(mode="json", by_alias=True, exclude_none=True)

    return descriptions


@dataclass
class Deps:
    available_endpoints: list[str]


def select_endpoint(openapi_spec: OpenAPI, user_query: str = ""):
    agent = Agent(
        "openai:gpt-3.5-turbo",
        retries=2,
        deps_type=Deps,
        result_type=str,
        instrument=True,
    )

    @agent.result_validator
    async def validate_result(ctx: RunContext[Deps], result: str) -> str:
        if result not in [*ctx.deps.available_endpoints, "null"]:
            raise ModelRetry(f"Invalid endpoint: {result}. Available endpoints: {ctx.deps.available_endpoints}")
        else:
            return result

    endpoint_descriptions = generate_endpoint_descriptions(openapi_spec)

    @agent.system_prompt
    async def system_prompt() -> str:
        nonlocal endpoint_descriptions
        return f"""\
    You are a smart assistant that can help pick the best endpoint from a list of endpoints based on
    a user's query. Given the list of endpoints and their openapi spec, you should look at the parameters
    and the user's query to see which endpoint can achieve the user's goal.

    Look at the inputs first, if the user is looking for the weather in a specific location for example,
    we should look for an endpoint that supports filtering by location. Otherwise none of the endpoints
    would be able to satisfy the user's query.

    Second, look at the response schemas. If the user wants the chance of rain for example and the output only
    provides temperature, then we should try to find another endpoint that better satisfies
    the user's query.

    Return a single endpoint string. Do not include your reasoning in the response.
    For example, if we have a list of endpoints:
    GET /tracking/<tracking_code>
    GET /messages
    and the user asks for the shipment details, you should return ONLY "/tracking/<tracking_code>", nothing else.

    If none of the endpoints seem to be able to satisfy the user's query, return 'null'.

    Available endpoints:
    {format_as_xml(endpoint_descriptions)}

    """

    available_endpoints = list(endpoint_descriptions.keys())
    deps = Deps(available_endpoints=available_endpoints)
    result = agent.run_sync(user_query, deps=deps)
    if result.data == "null":
        return None
    return result.data


def select_parameters(openapi_spec: OpenAPI, endpoint: str, user_query: str):
    """Given an endpoint, select the parameters to build the API call.
    This includes query and path parameters. Validation should be based on
    the endpoint's openapi spec.
    ex.
    {
        "sort": "updated",
        "direction": "desc",
        "state": "open",
        "since": {
            "type": "incremental",
            "cursor_path": "updated_at",
            "initial_value": "2024-01-25T11:21:28Z",
    }
    """

    # Extract endpoint parameters description
    if openapi_spec.paths and endpoint in openapi_spec.paths:
        endpoint_spec = openapi_spec.paths[endpoint]

        if endpoint_spec.get and endpoint_spec.get.parameters:
            parameter_descriptions = {}
            for parameter in endpoint_spec.get.parameters:
                # TODO: Handle references, not only resolved parameters
                if isinstance(parameter, ParameterV30 | ParameterV31):
                    parameter_descriptions[parameter.name] = parameter.model_dump(
                        mode="json", by_alias=True, exclude_none=True
                    )
        else:
            parameter_descriptions = {}
    else:
        raise ValueError(f"Endpoint {endpoint} not found in openapi spec")

    @dataclass
    class SelectParametersDeps:
        parameters: list[str]

    agent = Agent(
        "openai:gpt-3.5-turbo",
        retries=2,
        deps_type=SelectParametersDeps,
        result_type=dict[str, Any],
        instrument=True,
    )

    @agent.result_validator
    async def validate_result(ctx: RunContext[SelectParametersDeps], result: dict[str, Any]) -> dict[str, Any]:
        if result.keys() != ctx.deps.parameters:
            raise ModelRetry(f"Invalid parameters: {result.keys()}. Expected parameters: {ctx.deps.parameters}")
        else:
            return result

    @agent.system_prompt
    async def system_prompt() -> str:
        return f"""\
    You are a smart API engineer that can help pick the query and path parameter values to use to answer a user's query.

    We've got a list of parameters and their openapi spec, you should look at them and decide what values to use for the parameters.

    Return a dictionary of parameters with the parameter name as the key and the parameter value as the value.
    Return nothing else.

    Example:
    User query: "What's the weather in Berlin?"
    Endpoint: "/weather"
    Parameters:
    {{
        "location": "str",
        "timezone": "str",
    }}

    Response:
    {{
        "location": "Berlin",
        "timezone": "Europe/Berlin",
    }}

    Available parameters:
    {format_as_xml(parameter_descriptions)}
    """

    deps = SelectParametersDeps(parameters=list(parameter_descriptions.keys()))
    result = agent.run_sync(user_query, deps=deps)
    return result.data


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


def generate_endpoint_dlt_rest_api_source(openapi_spec: OpenAPI, endpoint: str):
    server_url = get_path_server_url(openapi_spec, endpoint)  # Verify server URL exists

    config: RESTAPIConfig = {
        "client": {
            "base_url": server_url,
            # TODO: Support optional headers
            # TODO: Support auth types
            # "auth": {
            #     "token": dlt.secrets["your_api_token"],
            # },
            # TODO: Support pagination
            # "paginator": {
            #     "type": "json_link",
            #     "next_url_path": "paging.next",
            # },
        },
        "resources": [
            # TODO: Support resource relationships
            # Only include the endpoint we selected
            {
                "name": endpoint,
                "endpoint": {
                    "path": endpoint,
                    "params": {},
                },
            },
        ],
    }

    return rest_api_resources(config)


def main():
    spec_url = (
        "https://raw.githubusercontent.com/open-meteo/open-meteo/3ff33913b216614c8c6751c18d336af3b1291f92/openapi.yml"
    )

    # Extract structure from openapi spec
    openapi_spec = parse_openapi_spec(url=spec_url)

    # Select single endpoint (exclude non-GET endpoints)
    query = "What's the weather in Berlin?"
    endpoint = select_endpoint(openapi_spec, user_query=query)
    # IMPROVE: Generate an explanation of why the endpoint was selected/not selected
    if endpoint is None:
        raise ValueError("No endpoint matches the query.")

    # Select parameters
    parameters = select_parameters(openapi_spec, endpoint, user_query=query)
    print(parameters)

    # source = generate_endpoint_dlt_rest_api_source(openapi_spec, endpoint)

    # pipeline = dlt.pipeline(pipeline_name="test", destination="duckdb", dataset_name="chat_1_call_1")
    # pipeline.run(source)


if __name__ == "__main__":
    main()
