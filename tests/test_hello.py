import json
import os
import sys
from unittest.mock import patch

import pytest

# Add src directory to path so we can import modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.functions.hello import handler


@pytest.fixture
def apigw_event():
    """Generates a simple API Gateway event"""
    return {
        "httpMethod": "GET",
        "path": "/hello",
        "requestContext": {"requestId": "test-id"},
        "headers": {"Content-Type": "application/json"},
        "queryStringParameters": {},
        "body": None,
    }


def test_hello_handler(apigw_event):
    # Mock environment variables
    with patch.dict(os.environ, {"STAGE": "test", "FUNCTION_NAME": "hello-test"}):
        # Call the handler
        response = handler.main(apigw_event, {})

        # Parse the response body
        body = json.loads(response.get("body", "{}"))

        # Verify the response
        assert response["statusCode"] == 200
        assert response["headers"]["Content-Type"] == "application/json"
        assert "message" in body
        assert body["message"] == "Hello from backend-core!"
        assert body["stage"] == "test"
        assert body["function_name"] == "hello-test"
