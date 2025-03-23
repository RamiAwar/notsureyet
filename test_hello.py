import json

import pytest
import requests
import responses
from dotenv import load_dotenv
from openapi_pydantic import OpenAPI, Parameter

from hello import (
    download_spec,
    generate_endpoint_descriptions,
    get_path_server_url,
    parse_openapi_spec,
    select_endpoint,
    select_parameters,
)

load_dotenv()

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
def simple_weather_api_spec():
    return {
        "openapi": "3.1.0",
        "info": {"title": "Weather API", "version": "1.0.0"},
        "paths": {
            "/forecast": {
                "get": {
                    "summary": "Get weather forecast",
                    "description": "Get a daily weather forecast",
                    "parameters": [{"name": "location", "in": "query", "required": True, "schema": {"type": "string"}}],
                    "responses": {
                        "200": {
                            "description": "Weather forecast",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "temperature": {"type": "number"},
                                            "precipitation": {"type": "number"},
                                        },
                                    }
                                }
                            },
                        }
                    },
                }
            },
            "/current": {
                "get": {
                    "summary": "Get current weather",
                    "description": "Get the current weather conditions",
                    "parameters": [{"name": "location", "in": "query", "required": True, "schema": {"type": "string"}}],
                    "responses": {
                        "200": {
                            "description": "Current weather",
                            "content": {
                                "application/json": {
                                    "schema": {"type": "object", "properties": {"temperature": {"type": "number"}}}
                                }
                            },
                        }
                    },
                }
            },
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


def test_parse_openapi_spec_valid(mock_responses, simple_weather_api_spec):
    mock_responses.add(
        responses.GET,
        "https://example.com/spec",
        body=json.dumps(simple_weather_api_spec),
        status=200,
        content_type="application/json",
    )

    result = parse_openapi_spec("https://example.com/spec")
    assert isinstance(result, OpenAPI)
    assert result.info is not None
    assert result.info.title == "Weather API"
    assert result.info.version == "1.0.0"
    assert result.paths is not None

    path_key = "/forecast"
    assert path_key in result.paths
    weather_path = result.paths[path_key]
    assert weather_path is not None
    assert weather_path.get is not None
    assert weather_path.get.summary == "Get weather forecast"
    assert weather_path.get.description == "Get a daily weather forecast"

    # Test parameters
    assert weather_path.get.parameters is not None
    params = weather_path.get.parameters
    assert len(params) == 1

    # Test path parameter
    path_param = next(p for p in params if isinstance(p, Parameter) and p.name == "location")
    assert isinstance(path_param, Parameter)
    assert path_param.param_in == "query"
    assert path_param.required is True


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


def test_get_server_url_raises_error_for_path_when_no_servers_defined(spec_without_servers):
    spec = OpenAPI.model_validate(spec_without_servers)
    spec.servers = []  # Manually remove root servers
    with pytest.raises(ValueError, match="No server url found in openapi spec"):
        get_path_server_url(spec, "/test")


def test_generate_endpoint_descriptions_with_get_endpoints(simple_weather_api_spec):
    spec = OpenAPI.model_validate(simple_weather_api_spec)
    descriptions = generate_endpoint_descriptions(spec)

    assert isinstance(descriptions, dict)
    assert len(descriptions) == 2
    assert "/forecast" in descriptions
    assert "/current" in descriptions

    path_desc = descriptions["/forecast"]
    assert path_desc["get"]["summary"] == "Get weather forecast"
    assert path_desc["get"]["description"] == "Get a daily weather forecast"
    assert len(path_desc["get"]["parameters"]) == 1

    # Verify parameters are included
    params = path_desc["get"]["parameters"]
    param_names = [p["name"] for p in params]
    assert "location" in param_names

    path_desc = descriptions["/current"]
    assert path_desc["get"]["summary"] == "Get current weather"
    assert path_desc["get"]["description"] == "Get the current weather conditions"
    assert len(path_desc["get"]["parameters"]) == 1

    # Verify parameters are included
    params = path_desc["get"]["parameters"]
    param_names = [p["name"] for p in params]
    assert "location" in param_names


def test_generate_endpoint_descriptions_with_empty_paths():
    spec = OpenAPI.model_validate({"openapi": "3.1.0", "info": {"title": "Empty API", "version": "1.0.0"}, "paths": {}})
    descriptions = generate_endpoint_descriptions(spec)
    assert descriptions == {}


def test_generate_endpoint_descriptions_with_mixed_methods():
    spec = OpenAPI.model_validate(
        {
            "openapi": "3.1.0",
            "info": {"title": "Mixed Methods API", "version": "1.0.0"},
            "paths": {
                "/users": {
                    "get": {"summary": "List users", "responses": {"200": {"description": "OK"}}},
                    "post": {"summary": "Create user", "responses": {"201": {"description": "Created"}}},
                },
                "/items": {"put": {"summary": "Update item", "responses": {"200": {"description": "OK"}}}},
            },
        }
    )
    descriptions = generate_endpoint_descriptions(spec)

    # Should only include paths with GET methods
    assert len(descriptions) == 1
    assert "/users" in descriptions
    assert "/items" not in descriptions  # PUT-only endpoint should be excluded


def test_generate_endpoint_descriptions_with_parameters():
    spec = OpenAPI.model_validate(
        {
            "openapi": "3.1.0",
            "info": {"title": "Parameterized API", "version": "1.0.0"},
            "paths": {
                "/users/{user_id}/posts": {
                    "get": {
                        "summary": "Get user posts",
                        "parameters": [
                            {"name": "user_id", "in": "path", "required": True, "schema": {"type": "string"}},
                            {
                                "name": "limit",
                                "in": "query",
                                "required": False,
                                "schema": {"type": "integer", "default": 10},
                            },
                        ],
                        "responses": {"200": {"description": "OK"}},
                    }
                }
            },
        }
    )
    descriptions = generate_endpoint_descriptions(spec)

    assert isinstance(descriptions, dict)
    assert len(descriptions) == 1
    assert "/users/{user_id}/posts" in descriptions

    path_desc = descriptions["/users/{user_id}/posts"]
    assert path_desc["get"]["summary"] == "Get user posts"

    # Verify parameters
    params = path_desc["get"]["parameters"]
    assert len(params) == 2

    # Check path parameter
    path_param = next(p for p in params if p["param_in"] == "path")
    assert path_param["name"] == "user_id"
    assert path_param["required"] is True
    assert path_param["param_schema"]["type"] == "string"

    # Check query parameter
    query_param = next(p for p in params if p["param_in"] == "query")
    assert query_param["name"] == "limit"
    assert query_param["required"] is False
    assert query_param["param_schema"]["type"] == "integer"
    assert query_param["param_schema"]["default"] == 10


def test_generate_endpoint_descriptions_with_no_get_endpoints():
    spec = OpenAPI.model_validate(
        {
            "openapi": "3.1.0",
            "info": {"title": "No GET API", "version": "1.0.0"},
            "paths": {
                "/users": {
                    "post": {"summary": "Create user", "responses": {"201": {"description": "Created"}}},
                    "put": {"summary": "Update user", "responses": {"200": {"description": "OK"}}},
                }
            },
        }
    )
    descriptions = generate_endpoint_descriptions(spec)
    assert descriptions == {}  # No GET endpoints should result in empty dict


def test_generate_endpoint_descriptions_with_path_level_parameters():
    spec = OpenAPI.model_validate(
        {
            "openapi": "3.1.0",
            "info": {"title": "Path Parameters API", "version": "1.0.0"},
            "paths": {
                "/organizations/{org_id}/users": {
                    "parameters": [{"name": "org_id", "in": "path", "required": True, "schema": {"type": "string"}}],
                    "get": {
                        "summary": "List organization users",
                        "parameters": [
                            {"name": "role", "in": "query", "required": False, "schema": {"type": "string"}}
                        ],
                        "responses": {"200": {"description": "OK"}},
                    },
                }
            },
        }
    )
    descriptions = generate_endpoint_descriptions(spec)

    assert len(descriptions) == 1
    path_desc = descriptions["/organizations/{org_id}/users"]

    # Verify both path-level and operation-level parameters are included
    assert "parameters" in path_desc
    assert len(path_desc["parameters"]) == 1
    assert path_desc["parameters"][0]["name"] == "org_id"

    assert len(path_desc["get"]["parameters"]) == 1
    assert path_desc["get"]["parameters"][0]["name"] == "role"


@pytest.mark.llm
def test_select_endpoint_for_forecast(simple_weather_api_spec):
    """Test that the endpoint selector chooses the forecast endpoint for future weather queries."""
    spec = OpenAPI.model_validate(simple_weather_api_spec)
    result = select_endpoint(spec, "What will the weather be like tomorrow in London?")
    assert result == "/forecast"


@pytest.mark.llm
def test_select_endpoint_for_current_weather(simple_weather_api_spec):
    """Test that the endpoint selector chooses the current endpoint for current weather queries."""
    spec = OpenAPI.model_validate(simple_weather_api_spec)
    result = select_endpoint(spec, "What's the current temperature in New York?")
    assert result == "/current"


@pytest.mark.llm
def test_select_endpoint_returns_none_for_unrelated_query(simple_weather_api_spec):
    """Test that the endpoint selector returns None for queries unrelated to available endpoints."""
    spec = OpenAPI.model_validate(simple_weather_api_spec)
    result = select_endpoint(spec, "What's the stock price of AAPL?")
    assert result is None


def test_select_endpoint_returns_none():
    spec = OpenAPI.model_validate({"openapi": "3.1.0", "info": {"title": "Test API", "version": "1.0.0"}, "paths": {}})
    assert select_endpoint(spec) is None
    assert select_endpoint(spec, "some query") is None
