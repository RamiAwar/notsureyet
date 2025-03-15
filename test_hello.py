import pytest
import requests
import responses
from openapi_pydantic import OpenAPI, Parameter, Schema

from hello import download_spec, parse_openapi_spec

# Test URLs
YAML_URL = "https://raw.githubusercontent.com/open-meteo/open-meteo/refs/heads/main/openapi.yml"
JSON_URL = "https://raw.githubusercontent.com/OAI/OpenAPI-Specification/main/schemas/v2.0/schema.json"


@pytest.fixture
def mock_responses():
    with responses.RequestsMock() as rsps:
        yield rsps


def test_download_spec_yaml(mock_responses):
    # Sample YAML content
    yaml_content = """
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
    mock_responses.add(responses.GET, YAML_URL, body=yaml_content, status=200, content_type="text/yaml")

    result = download_spec(YAML_URL)
    assert isinstance(result, dict)
    assert result["openapi"] == "3.0.0"
    assert result["info"]["title"] == "Sample API"
    assert result["paths"]


def test_download_spec_json(mock_responses):
    # Sample JSON content
    json_content = """
    {
        "swagger": "2.0",
        "info": {
            "title": "Sample API",
            "version": "1.0.0"
        },
        "paths": {
            "/hello": {
                "get": {
                    "summary": "Hello World",
                    "responses": {
                        "200": {
                            "description": "OK"
                        }
                    }
                }
            }
        }
    }
    """
    mock_responses.add(responses.GET, JSON_URL, body=json_content, status=200, content_type="application/json")

    result = download_spec(JSON_URL)
    assert isinstance(result, dict)
    assert result["swagger"] == "2.0"
    assert result["info"]["title"] == "Sample API"
    assert result["paths"]


def test_download_spec_invalid_url(mock_responses):
    mock_responses.add(responses.GET, "https://invalid-url.com/spec", status=404)

    with pytest.raises(requests.exceptions.HTTPError):
        download_spec("https://invalid-url.com/spec")


def test_download_spec_invalid_content(mock_responses):
    # Invalid content that's neither JSON nor YAML
    mock_responses.add(
        responses.GET,
        "https://example.com/spec",
        body="{\n  'invalid': 'json\n  with: invalid yaml",  # Both invalid JSON and invalid YAML
        status=200,
    )

    with pytest.raises(ValueError, match="Failed to parse file as json or yaml"):
        download_spec("https://example.com/spec")


def test_parse_openapi_spec_valid(mock_responses):
    # Valid OpenAPI 3.1 spec
    spec_content = """
    {
        "openapi": "3.1.0",
        "info": {
            "title": "Weather API",
            "version": "1.0.0",
            "description": "API for weather forecasts"
        },
        "servers": [
            {
                "url": "https://api.example.com/v1"
            }
        ],
        "paths": {
            "/locations/{location_id}/weather": {
                "get": {
                    "summary": "Get weather forecast",
                    "description": "Get detailed weather forecast for a specific location",
                    "parameters": [
                        {
                            "name": "location_id",
                            "in": "path",
                            "required": true,
                            "description": "The unique identifier of the location",
                            "schema": {
                                "type": "string"
                            }
                        },
                        {
                            "name": "latitude",
                            "in": "query",
                            "required": true,
                            "description": "WGS84 coordinate latitude",
                            "schema": {
                                "type": "number",
                                "format": "float"
                            }
                        },
                        {
                            "name": "longitude",
                            "in": "query",
                            "required": true,
                            "description": "WGS84 coordinate longitude",
                            "schema": {
                                "type": "number",
                                "format": "float"
                            }
                        },
                        {
                            "name": "units",
                            "in": "query",
                            "required": false,
                            "description": "Temperature unit to use",
                            "schema": {
                                "type": "string",
                                "enum": ["celsius", "fahrenheit"],
                                "default": "celsius"
                            }
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "Successful response",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "temperature": {
                                                "type": "number"
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    """
    mock_responses.add(
        responses.GET, "https://example.com/spec", body=spec_content, status=200, content_type="application/json"
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


def test_parse_openapi_spec_invalid_schema(mock_responses):
    # Invalid OpenAPI spec - missing required fields
    spec_content = """
    {
        "openapi": "3.1.0",
        "paths": {
            "/weather": {
                "get": {
                    "responses": {
                        "200": {
                            "description": "OK"
                        }
                    }
                }
            }
        }
    }
    """
    mock_responses.add(
        responses.GET, "https://example.com/spec", body=spec_content, status=200, content_type="application/json"
    )

    # Should raise validation error because 'info' is required
    with pytest.raises(Exception) as exc_info:
        parse_openapi_spec("https://example.com/spec")
    assert "info" in str(exc_info.value).lower()  # Error should mention missing 'info' field
