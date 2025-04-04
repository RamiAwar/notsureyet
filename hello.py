import json
import os
from dataclasses import dataclass
from typing import Annotated, Any, Literal

import dlt
import logfire
import requests
import yaml
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import SinglePagePaginator
from mirascope import llm
from mirascope.retries.tenacity import collect_errors
from openapi_pydantic.v3.v3_0 import OpenAPI as OpenAPI30
from openapi_pydantic.v3.v3_0 import Parameter as ParameterV30
from openapi_pydantic.v3.v3_1 import OpenAPI as OpenAPI31
from openapi_pydantic.v3.v3_1 import Parameter as ParameterV31
from pydantic import AfterValidator, BaseModel, ValidationError
from pydantic_ai import Agent, ModelRetry, RunContext
from pydantic_ai.format_as_xml import format_as_xml
from tenacity import retry, stop_after_attempt

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


class ParameterIn(BaseModel):
    name: str
    param_in: Literal["query", "path"]
    value: str


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


def resolve_parameter_reference(parameter: dict, openapi_spec: OpenAPI) -> ParameterV30 | ParameterV31 | None:
    """Resolve a parameter reference in the OpenAPI spec."""
    if isinstance(parameter, dict) and "$ref" in parameter:
        ref_name = parameter["$ref"].split("/")[-1]
        return openapi_spec.components.parameters.get(ref_name)
    return parameter


def select_parameters(openapi_spec: OpenAPI, endpoint: str, user_query: str) -> list[ParameterIn]:
    """Given an endpoint, select the parameters to build the API call.
    This includes query and path parameters. Validation should be based on
    the endpoint's openapi spec.
    """
    # Extract endpoint parameters description
    if openapi_spec.paths and endpoint in openapi_spec.paths:
        endpoint_spec = openapi_spec.paths[endpoint]
        if endpoint_spec.get and endpoint_spec.get.parameters:
            parameter_descriptions = {}
            for parameter in endpoint_spec.get.parameters:
                # Use the new function to resolve references
                resolved_parameter = resolve_parameter_reference(parameter, openapi_spec)
                if isinstance(resolved_parameter, ParameterV30 | ParameterV31):
                    parameter_descriptions[resolved_parameter.name] = resolved_parameter.model_dump(
                        mode="json", by_alias=True, exclude_none=True
                    )
        else:
            parameter_descriptions = {}
    else:
        raise ValueError(f"Endpoint {endpoint} not found in openapi spec")

    # If no parameters, return empty list immediately without calling LLM
    if not parameter_descriptions:
        return []

    def system_prompt(parameter_descriptions: dict[str, Any]) -> str:
        return f"""\
    You are a smart API engineer that can help pick the query and path parameter values to use to answer a user's query.
    We've got a list of parameters and their openapi spec, you should look at them and decide what values to use for the parameters.
    Return a dictionary of a single key "parameters" that has a list of key value pairs with the parameter name as the key and the parameter value as the value. DO NOT
    pick parameters that are not in the provided list or values that do not match the spec. Go over the parameters you pick and see if there's a need
    to provide additional ones that the user didn't think of but are required to get the right response. For example, if 
    the user asks for the weather, we should also see how to return it (hourly, daily, etc) and add that to the parameters.

    Example:
    User query: "What's the weather in Berlin?"
    Endpoint: "/weather"
    Parameters:
    {{
        "location": "str",
        "timezone": "str",
        "frequency": "str",
    }}
    Response:
    {{
        "parameters": [
            {{
                "name": "location",
                "param_in": "query",
                "value": "Berlin"
            }},
            {{
                "name": "timezone",
                "param_in": "query",
                "value": "Europe/Berlin"
            }},
            {{
                "name": "frequency",
                "param_in": "query",
                "value": "daily"
            }}
        ]
    }}
    Available parameters:
    {format_as_xml(parameter_descriptions)}
    User query:
    {user_query}
    """

    def _validate_parameter_names(v: list[ParameterIn]) -> list[ParameterIn]:
        nonlocal parameter_descriptions
        invalid_names = set()
        for param in v:
            # Check if the parameter name is in the descriptions
            if param.name not in parameter_descriptions:
                invalid_names.add(param.name)
            
        # Construct specific error
        if invalid_names:
            raise ValueError(
                f"Invalid parameter names: {', '.join(invalid_names)}. "
                f"Available parameters: {', '.join(parameter_descriptions.keys())}"
            )

        return v

    class GetParamsResult(BaseModel):
        parameters: Annotated[list[ParameterIn], AfterValidator(_validate_parameter_names)]

    @retry(stop=stop_after_attempt(3), after=collect_errors(ValidationError))
    @llm.call(
        provider="openai",
        model="gpt-3.5-turbo",
        json_mode=True,
        response_model=GetParamsResult,
    )
    def llm_get_params(parameter_descriptions: dict[str, Any], user_query: str, *, errors: list[ValidationError] | None = None) -> str:
        if errors:
            print("errors", errors)
            return f"Previous Error: {errors}\n\n{system_prompt(parameter_descriptions)}"
        return system_prompt(parameter_descriptions)

    result = llm_get_params(parameter_descriptions=parameter_descriptions, user_query=user_query)
    return result.parameters


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


def generate_endpoint_dlt_rest_api_source(openapi_spec: OpenAPI, endpoint: str, parameters: list[ParameterIn]) -> dlt.source:
    # TODO: Verify server URL exists
    server_url = get_path_server_url(openapi_spec, endpoint)

    config: RESTAPIConfig = {
        "client": {
            "base_url": server_url,
            # TODO: Support optional headers
            # TODO: Support auth types
            # "auth": {
            #     "token": dlt.secrets["your_api_token"],
            # },
            # TODO: Support pagination
            "paginator": SinglePagePaginator(),
        },
        "resources": [
            # TODO: Support resource relationships
            # TODO: Handle header/cookie params
            # Only include the endpoint we selected
            {
                "name": endpoint,
                "endpoint": {
                    "path": endpoint,
                    "params": {
                        p.name: p.value for p in parameters if p.param_in in ["query", "path"]
                    },
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
    query = "What's the hourly weather at 52.52,13.41 (berlin)?"
    endpoint = select_endpoint(openapi_spec, user_query=query)
    # IMPROVE: Generate an explanation of why the endpoint was selected/not selected
    if endpoint is None:
        raise ValueError("No endpoint matches the query.")

    # Select parameters
    parameters = select_parameters(openapi_spec, endpoint, user_query=query)
    print(parameters)

    source = generate_endpoint_dlt_rest_api_source(openapi_spec, endpoint, parameters)
    
    client = RESTClient(base_url=get_path_server_url(openapi_spec, endpoint))
    response = client.get(endpoint, params={p.name: p.value for p in parameters if p.param_in == "query"})
    print(response.json())

    pipeline = dlt.pipeline(pipeline_name="test", destination="duckdb", dataset_name="chat2")
    pipeline.run(source)


if __name__ == "__main__":
    main()
