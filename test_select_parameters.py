import pytest
from hello import select_parameters
from openapi_pydantic import OpenAPI

@pytest.fixture
def openapi_spec():
    """Fixture to provide a manually written OpenAPI spec for testing."""
    return OpenAPI.model_validate({
        "openapi": "3.1.0",
        "info": {"title": "Weather API", "version": "1.0.0"},
        "paths": {
            "/forecast": {
                "get": {
                    "summary": "Get weather forecast",
                    "parameters": [
                        {"name": "latitude", "in": "query", "required": True, "schema": {"type": "number"}},
                        {"name": "longitude", "in": "query", "required": True, "schema": {"type": "number"}},
                    ],
                    "responses": {"200": {"description": "Weather forecast"}},
                }
            },
            "/health": {
                "get": {
                    "summary": "Health check",
                    "responses": {"200": {"description": "OK"}},
                }
            },
        },
    })

@pytest.mark.llm
def test_select_parameters_basic(openapi_spec):
    """Test select_parameters with a manually written OpenAPI spec and valid inputs."""
    endpoint = "/forecast"
    user_query = "Get the weather forecast for Berlin"

    result = select_parameters(openapi_spec, endpoint, user_query)

    assert len(result) > 0, "Expected parameters to be selected"
    assert any(param.name == "latitude" for param in result), "Expected 'latitude' parameter to be selected"
    assert any(param.name == "longitude" for param in result), "Expected 'longitude' parameter to be selected"


@pytest.mark.llm
def test_select_parameters_no_parameters(openapi_spec):
    """Test select_parameters with an endpoint that has no parameters."""
    endpoint = "/health"
    user_query = "Check the health status"

    result = select_parameters(openapi_spec, endpoint, user_query)

    assert result == [], "Expected no parameters to be selected for an endpoint with no parameters"


@pytest.mark.llm
def test_select_parameters_invalid_endpoint(openapi_spec):
    """Test select_parameters with an invalid endpoint."""
    endpoint = "/invalid-endpoint"
    user_query = "This endpoint does not exist"

    with pytest.raises(ValueError, match=f"Endpoint {endpoint} not found in openapi spec"):
        select_parameters(openapi_spec, endpoint, user_query)