import json

import pytest
import requests
import responses
from openapi_pydantic import OpenAPI, Parameter, Schema

from hello import (
    download_spec,
    generate_endpoint_descriptions,
    get_path_server_url,
    openapi_to_restapi_source,
    parse_openapi_spec,
    select_endpoint,
    select_parameters,
)

# Test URLs
YAML_URL = "https://raw.githubusercontent.com/open-meteo/open-meteo/refs/heads/main/openapi.yml"
JSON_URL = "https://raw.githubusercontent.com/OAI/OpenAPI-Specification/main/schemas/v2.0/schema.json"


@pytest.fixture
def mock_responses():
    with responses.RequestsMock() as rsps:
        yield rsps


@pytest.fixture
def basic_spec_content():
    return """
    openapi: 3.0.0
    info:
      title: Sample API
      version: 1.0.0
    paths:
      /hello:
        get:
          summary: Hello World
          responses:
            '200':
              description: OK
    """


@pytest.fixture
def basic_json_spec():
    return {
        "swagger": "2.0",
        "info": {"title": "Sample API", "version": "1.0.0"},
        "paths": {"/hello": {"get": {"summary": "Hello World", "responses": {"200": {"description": "OK"}}}}},
    }


@pytest.fixture
def invalid_spec_content():
    return "{\n  'invalid': 'json\n  with: invalid yaml"


@pytest.fixture
def invalid_openapi_spec():
    return {"openapi": "3.1.0", "paths": {"/weather": {"get": {"responses": {"200": {"description": "OK"}}}}}}


@pytest.fixture
def weather_api_spec():
    return {
        "openapi": "3.1.0",
        "info": {"title": "Weather API", "version": "1.0.0", "description": "API for weather forecasts"},
        "servers": [{"url": "https://api.example.com/v1"}],
        "paths": {
            "/locations/{location_id}/weather": {
                "get": {
                    "summary": "Get weather forecast",
                    "description": "Get detailed weather forecast for a specific location",
                    "parameters": [
                        {
                            "name": "location_id",
                            "in": "path",
                            "required": True,
                            "description": "The unique identifier of the location",
                            "schema": {"type": "string"},
                        },
                        {
                            "name": "latitude",
                            "in": "query",
                            "required": True,
                            "description": "WGS84 coordinate latitude",
                            "schema": {"type": "number", "format": "float"},
                        },
                        {
                            "name": "longitude",
                            "in": "query",
                            "required": True,
                            "description": "WGS84 coordinate longitude",
                            "schema": {"type": "number", "format": "float"},
                        },
                        {
                            "name": "units",
                            "in": "query",
                            "required": False,
                            "description": "Temperature unit to use",
                            "schema": {"type": "string", "enum": ["celsius", "fahrenheit"], "default": "celsius"},
                        },
                    ],
                    "responses": {
                        "200": {
                            "description": "Successful response",
                            "content": {
                                "application/json": {
                                    "schema": {"type": "object", "properties": {"temperature": {"type": "number"}}}
                                }
                            },
                        }
                    },
                }
            }
        },
    }


@pytest.fixture
def spec_with_root_servers():
    return {
        "openapi": "3.1.0",
        "info": {"title": "Test API", "version": "1.0.0"},
        "servers": [
            {"url": "https://api.example.com/v1", "description": "Production server"},
            {"url": "https://staging.example.com/v1", "description": "Staging server"},
        ],
        "paths": {},
    }


@pytest.fixture
def spec_with_path_servers():
    return {
        "openapi": "3.1.0",
        "info": {"title": "Test API", "version": "1.0.0"},
        "paths": {
            "/weather": {
                "servers": [{"url": "https://weather.example.com/v1", "description": "Weather API server"}],
                "get": {"summary": "Get weather"},
            },
            "/other": {
                "servers": [{"url": "https://other.example.com/v1", "description": "Other API server"}],
                "get": {"summary": "Get other"},
            },
        },
    }


@pytest.fixture
def spec_without_servers():
    return {
        "openapi": "3.1.0",
        "info": {"title": "Test API", "version": "1.0.0"},
        "paths": {"/test": {"get": {"summary": "Test endpoint"}}},
    }


def test_download_spec_yaml(mock_responses, basic_spec_content):
    mock_responses.add(responses.GET, YAML_URL, body=basic_spec_content, status=200, content_type="text/yaml")

    result = download_spec(YAML_URL)
    assert isinstance(result, dict)
    assert result["openapi"] == "3.0.0"
    assert result["info"]["title"] == "Sample API"
    assert result["paths"]


def test_download_spec_json(mock_responses, basic_json_spec):
    mock_responses.add(
        responses.GET, JSON_URL, body=json.dumps(basic_json_spec), status=200, content_type="application/json"
    )

    result = download_spec(JSON_URL)
    assert isinstance(result, dict)
    assert result["swagger"] == "2.0"
    assert result["info"]["title"] == "Sample API"
    assert result["paths"]


def test_download_spec_invalid_url(mock_responses):
    mock_responses.add(responses.GET, "https://invalid-url.com/spec", status=404)

    with pytest.raises(requests.exceptions.HTTPError):
        download_spec("https://invalid-url.com/spec")


def test_download_spec_invalid_content(mock_responses, invalid_spec_content):
    mock_responses.add(
        responses.GET,
        "https://example.com/spec",
        body=invalid_spec_content,
        status=200,
    )

    with pytest.raises(ValueError, match="Failed to parse file as json or yaml"):
        download_spec("https://example.com/spec")


def test_parse_openapi_spec_valid(mock_responses, weather_api_spec):
    mock_responses.add(
        responses.GET,
        "https://example.com/spec",
        body=json.dumps(weather_api_spec),
        status=200,
        content_type="application/json",
    )

    result = parse_openapi_spec("https://example.com/spec")
    assert isinstance(result, OpenAPI)
    assert result.info is not None
    assert result.info.title == "Weather API"
    assert result.info.version == "1.0.0"
    assert result.paths is not None

    path_key = "/locations/{location_id}/weather"
    assert path_key in result.paths
    weather_path = result.paths[path_key]
    assert weather_path is not None
    assert weather_path.get is not None
    assert weather_path.get.summary == "Get weather forecast"
    assert weather_path.get.description == "Get detailed weather forecast for a specific location"

    # Test parameters
    assert weather_path.get.parameters is not None
    params = weather_path.get.parameters
    assert len(params) == 4

    # Test path parameter
    path_param = next(p for p in params if isinstance(p, Parameter) and p.name == "location_id")
    assert isinstance(path_param, Parameter)
    assert path_param.param_in == "path"
    assert path_param.required is True
    assert path_param.description == "The unique identifier of the location"

    # Test required query parameters
    lat_param = next(p for p in params if isinstance(p, Parameter) and p.name == "latitude")
    assert isinstance(lat_param, Parameter)
    assert lat_param.param_in == "query"
    assert lat_param.required is True
    assert lat_param.description == "WGS84 coordinate latitude"
    assert isinstance(lat_param.param_schema, Schema)
    assert lat_param.param_schema.type == "number"
    assert lat_param.param_schema.schema_format == "float"

    # Test optional parameter with enum
    units_param = next(p for p in params if isinstance(p, Parameter) and p.name == "units")
    assert isinstance(units_param, Parameter)
    assert units_param.param_in == "query"
    assert units_param.required is False
    assert units_param.description == "Temperature unit to use"
    assert isinstance(units_param.param_schema, Schema)
    assert units_param.param_schema.type == "string"
    assert units_param.param_schema.enum == ["celsius", "fahrenheit"]
    assert units_param.param_schema.default == "celsius"


def test_parse_openapi_spec_invalid_schema(mock_responses, invalid_openapi_spec):
    mock_responses.add(
        responses.GET,
        "https://example.com/spec",
        body=json.dumps(invalid_openapi_spec),
        status=200,
        content_type="application/json",
    )

    # Should raise validation error because 'info' is required
    with pytest.raises(Exception) as exc_info:
        parse_openapi_spec("https://example.com/spec")
    assert "info" in str(exc_info.value).lower()  # Error should mention missing 'info' field


def test_get_server_url_uses_root_server_when_available(spec_with_root_servers):
    spec = OpenAPI.model_validate(spec_with_root_servers)
    assert get_path_server_url(spec) == "https://api.example.com/v1"


def test_get_server_url_uses_root_server_when_path_has_no_override(spec_with_root_servers):
    spec = OpenAPI.model_validate(spec_with_root_servers)
    assert get_path_server_url(spec, "/some/path") == "https://api.example.com/v1"


def test_get_server_url_uses_path_server_when_available(spec_with_path_servers):
    spec = OpenAPI.model_validate(spec_with_path_servers)
    assert get_path_server_url(spec, "/weather") == "https://weather.example.com/v1"


def test_get_server_url_raises_error_for_path_without_server(spec_with_path_servers):
    spec = OpenAPI.model_validate(spec_with_path_servers)
    spec.servers = []  # Manually remove root servers
    with pytest.raises(ValueError, match="No server url found in openapi spec"):
        get_path_server_url(spec, "/nonexistent")


def test_get_server_url_raises_error_when_no_path_and_no_root_servers(spec_with_path_servers):
    spec = OpenAPI.model_validate(spec_with_path_servers)
    spec.servers = []  # Manually remove root servers
    with pytest.raises(ValueError, match="No server url found in openapi spec"):
        get_path_server_url(spec)


def test_get_server_url_raises_error_when_no_servers_defined(spec_without_servers):
    spec = OpenAPI.model_validate(spec_without_servers)
    spec.servers = []  # Manually remove root servers
    with pytest.raises(ValueError, match="No server url found in openapi spec"):
        get_path_server_url(spec)


def test_get_server_url_raises_error_for_path_when_no_servers_defined(spec_without_servers):
    spec = OpenAPI.model_validate(spec_without_servers)
    spec.servers = []  # Manually remove root servers
    with pytest.raises(ValueError, match="No server url found in openapi spec"):
        get_path_server_url(spec, "/test")


def test_generate_endpoint_descriptions_with_get_endpoints(weather_api_spec):
    spec = OpenAPI.model_validate(weather_api_spec)
    # Currently the function doesn't return anything, so we just verify it runs without error
    generate_endpoint_descriptions(spec)


def test_generate_endpoint_descriptions_with_empty_paths():
    spec = OpenAPI.model_validate({"openapi": "3.1.0", "info": {"title": "Empty API", "version": "1.0.0"}, "paths": {}})
    generate_endpoint_descriptions(spec)


def test_select_endpoint_returns_none():
    spec = OpenAPI.model_validate({"openapi": "3.1.0", "info": {"title": "Test API", "version": "1.0.0"}, "paths": {}})
    assert select_endpoint(spec) is None
    assert select_endpoint(spec, "some query") is None


def test_select_parameters_returns_none():
    spec = OpenAPI.model_validate({"openapi": "3.1.0", "info": {"title": "Test API", "version": "1.0.0"}, "paths": {}})
    assert select_parameters(spec, "/some/path") is None


def test_openapi_to_restapi_source_returns_empty_list(weather_api_spec):
    spec = OpenAPI.model_validate(weather_api_spec)
    assert openapi_to_restapi_source(spec) == []
    assert openapi_to_restapi_source(spec, "/some/path") == []
