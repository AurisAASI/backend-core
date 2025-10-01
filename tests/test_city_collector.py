"""
Test cases for the city_collector lambda function.

This test file validates that the city_collector function can handle
both API Gateway and EventBridge event formats correctly.
"""

import json
import os
import sys
import unittest
import importlib.util
from unittest.mock import patch, MagicMock

# Add src directory to the path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

# We need to import the module directly since the package name has hyphens
# This is a workaround for packages with hyphens which are not valid Python identifiers
spec = importlib.util.spec_from_file_location(
    "handler", 
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../src/functions/core-city-collector/handler.py"))
)
handler_module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = handler_module
spec.loader.exec_module(handler_module)
city_collector = handler_module.city_collector


class TestCityCollector(unittest.TestCase):
    """Tests for city_collector lambda function."""

    def setUp(self):
        """Set up test fixtures."""
        self.context = MagicMock()
        self.valid_payload = {
            "city_name": "Austin",
            "state_name": "Texas"
        }

    def test_api_gateway_event(self):
        """Test handling of API Gateway event."""
        # Create API Gateway event format
        event = {
            "httpMethod": "POST",
            "body": json.dumps(self.valid_payload),
            "headers": {
                "Content-Type": "application/json"
            }
        }

        response = city_collector(event, self.context)
        self.assertEqual(response["statusCode"], 200)
        
        body = json.loads(response["body"])
        self.assertEqual(body["city_name"], "Austin")
        self.assertEqual(body["state_name"], "Texas")

    def test_eventbridge_event(self):
        """Test handling of EventBridge event."""
        # Create EventBridge event format
        event = {
            "source": "aws.events",
            "detail-type": "Scheduled Event",
            "detail": self.valid_payload
        }

        response = city_collector(event, self.context)
        self.assertEqual(response["success"], True)
        self.assertEqual(response["data"]["city_name"], "Austin")
        self.assertEqual(response["data"]["state_name"], "Texas")

    def test_direct_invocation_event(self):
        """Test handling of direct invocation with payload."""
        # Direct invocation without API Gateway or EventBridge wrapper
        event = self.valid_payload

        response = city_collector(event, self.context)
        self.assertEqual(response["success"], True)
        self.assertEqual(response["data"]["city_name"], "Austin")
        self.assertEqual(response["data"]["state_name"], "Texas")

    def test_missing_city_name(self):
        """Test validation when city_name is missing."""
        # Create event with missing city_name
        event = {
            "body": json.dumps({"state_name": "Texas"})
        }

        response = city_collector(event, self.context)
        self.assertEqual(response["statusCode"], 400)
        
        body = json.loads(response["body"])
        self.assertIn("error", body)
        self.assertIn("city_name", body["error"])

    def test_missing_state_name(self):
        """Test validation when state_name is missing."""
        # Create event with missing state_name
        event = {
            "body": json.dumps({"city_name": "Austin"})
        }

        response = city_collector(event, self.context)
        self.assertEqual(response["statusCode"], 400)
        
        body = json.loads(response["body"])
        self.assertIn("error", body)
        self.assertIn("state_name", body["error"])


if __name__ == "__main__":
    unittest.main()