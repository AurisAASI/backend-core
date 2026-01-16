"""
Test cases for the city_collector lambda function.

This test file validates that the city_collector function can handle
both API Gateway and EventBridge event formats correctly, and properly
integrates with external APIs and AWS services.
"""

import importlib.util
import json
import os
import sys
from http import HTTPStatus
from unittest.mock import MagicMock, Mock, patch

import pytest

# Add src directory to the path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

# Import the handler module
spec = importlib.util.spec_from_file_location(
    'handler',
    os.path.abspath(
        os.path.join(
            os.path.dirname(__file__), '../src/functions/city_collector/handler.py'
        )
    ),
)
handler_module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = handler_module
spec.loader.exec_module(handler_module)
city_collector = handler_module.city_collector
extract_payload = handler_module.extract_payload
validate_payload = handler_module.validate_payload


# Fixtures
@pytest.fixture
def lambda_context():
    """Mock Lambda context object."""
    context = MagicMock()
    context.function_name = 'city-collector'
    context.memory_limit_in_mb = 128
    context.invoked_function_arn = (
        'arn:aws:lambda:us-east-1:123456789012:function:city-collector'
    )
    context.aws_request_id = 'test-request-id'
    return context


@pytest.fixture
def valid_payload():
    """Valid city, state, and niche payload."""
    return {'city': 'São Paulo', 'state': 'SP', 'niche': 'Technology'}


@pytest.fixture
def mock_brasil_api_success():
    """Mock successful Brasil API responses."""
    with patch('requests.get') as mock_get:
        # Mock state validation response
        mock_state_response = Mock()
        mock_state_response.json.return_value = {
            'id': 35,
            'sigla': 'SP',
            'nome': 'São Paulo',
            'regiao': {'id': 3, 'sigla': 'SE', 'nome': 'Sudeste'},
        }

        # Mock city validation response
        mock_city_response = Mock()
        mock_city_response.json.return_value = [
            {'codigo_ibge': '3550308', 'nome': 'São Paulo'},
            {'codigo_ibge': '3509502', 'nome': 'Campinas'},
            {'codigo_ibge': '3518800', 'nome': 'Guarulhos'},
        ]

        mock_get.side_effect = [mock_state_response, mock_city_response]
        yield mock_get


@pytest.fixture
def mock_sqs_client():
    """Mock SQS client."""
    with patch('boto3.client') as mock_boto_client:
        mock_sqs = MagicMock()
        mock_sqs.send_message.return_value = {
            'MessageId': 'test-message-id-12345',
            'MD5OfMessageBody': 'test-md5',
        }
        mock_boto_client.return_value = mock_sqs
        yield mock_sqs


@pytest.fixture
def mock_env_vars(monkeypatch):
    """Mock environment variables."""
    monkeypatch.setenv(
        'SCRAPER_TASK_QUEUE_URL',
        'https://sqs.us-east-1.amazonaws.com/123456789012/scraper-queue',
    )
    monkeypatch.setenv('STAGE', 'test')
    monkeypatch.setenv('FUNCTION_NAME', 'city-collector-test')


# Tests for extract_payload function
class TestExtractPayload:
    """Tests for the extract_payload helper function."""

    def test_extract_from_api_gateway_string_body(self, valid_payload):
        """Test extracting payload from API Gateway event with string body."""
        event = {'httpMethod': 'POST', 'body': json.dumps(valid_payload)}
        result = extract_payload(event)
        assert result == valid_payload

    def test_extract_from_api_gateway_dict_body(self, valid_payload):
        """Test extracting payload from API Gateway event with dict body."""
        event = {'httpMethod': 'POST', 'body': valid_payload}
        result = extract_payload(event)
        assert result == valid_payload

    def test_extract_from_eventbridge(self, valid_payload):
        """Test extracting payload from EventBridge event."""
        event = {
            'source': 'aws.events',
            'detail-type': 'Scheduled Event',
            'detail': valid_payload,
        }
        result = extract_payload(event)
        assert result == valid_payload

    def test_extract_from_direct_invocation(self, valid_payload):
        """Test extracting payload from direct invocation."""
        result = extract_payload(valid_payload)
        assert result == valid_payload


# Tests for validate_payload function
class TestValidatePayload:
    """Tests for the validate_payload helper function."""

    def test_validate_valid_payload(self, valid_payload):
        """Test validation passes for valid payload."""
        result = validate_payload(valid_payload)
        assert result is None

    def test_validate_missing_city(self):
        """Test validation fails when city is missing."""
        payload = {'state': 'SP', 'niche': 'Technology'}
        result = validate_payload(payload)
        assert result is not None
        assert 'city' in result['error']

    def test_validate_missing_state(self):
        """Test validation fails when state is missing."""
        payload = {'city': 'São Paulo', 'niche': 'Technology'}
        result = validate_payload(payload)
        assert result is not None
        assert 'state' in result['error']

    def test_validate_missing_niche(self):
        """Test validation fails when niche is missing."""
        payload = {'city': 'São Paulo', 'state': 'SP'}
        result = validate_payload(payload)
        assert result is not None
        assert 'niche' in result['error']

    def test_validate_empty_payload(self):
        """Test validation fails for empty payload."""
        result = validate_payload({})
        assert result is not None
        assert 'error' in result


# Tests for city_collector function
class TestCityCollector:
    """Tests for the main city_collector lambda function."""

    def test_api_gateway_event_success(
        self,
        lambda_context,
        valid_payload,
        mock_brasil_api_success,
        mock_sqs_client,
        mock_env_vars,
    ):
        """Test successful handling of API Gateway event."""
        event = {
            'httpMethod': 'POST',
            'body': json.dumps(valid_payload),
            'headers': {'Content-Type': 'application/json'},
        }

        response = city_collector(event, lambda_context)

        assert response['statusCode'] == 200
        assert 'Content-Type' in response['headers']
        assert 'Access-Control-Allow-Origin' in response['headers']

        body = json.loads(response['body'])
        assert body['message'] == 'City collection initiated'
        assert body['city'] == valid_payload['city'].upper()
        assert body['state'] == valid_payload['state'].upper()
        assert body['niche'] == valid_payload['niche'].upper()

        # Verify SQS was called
        mock_sqs_client.send_message.assert_called_once()

    def test_eventbridge_event_success(
        self,
        lambda_context,
        valid_payload,
        mock_brasil_api_success,
        mock_sqs_client,
        mock_env_vars,
    ):
        """Test successful handling of EventBridge event."""
        event = {
            'source': 'aws.events',
            'detail-type': 'Scheduled Event',
            'detail': valid_payload,
        }

        response = city_collector(event, lambda_context)

        assert response['success'] is True
        assert 'data' in response
        assert response['data']['city'] == valid_payload['city'].upper()
        assert response['data']['state'] == valid_payload['state'].upper()
        assert response['data']['niche'] == valid_payload['niche'].upper()

        # Verify SQS was called
        mock_sqs_client.send_message.assert_called_once()

    def test_direct_invocation_success(
        self,
        lambda_context,
        valid_payload,
        mock_brasil_api_success,
        mock_sqs_client,
        mock_env_vars,
    ):
        """Test successful handling of direct invocation."""
        response = city_collector(valid_payload, lambda_context)

        assert response['success'] is True
        assert 'data' in response

        # Verify SQS was called
        mock_sqs_client.send_message.assert_called_once()

    def test_missing_city_api_gateway(self, lambda_context):
        """Test API Gateway request fails when city is missing."""
        event = {
            'httpMethod': 'POST',
            'body': json.dumps({'state': 'SP', 'niche': 'Technology'}),
        }

        response = city_collector(event, lambda_context)

        assert response['statusCode'] == 400
        body = json.loads(response['body'])
        assert 'error' in body
        assert 'city' in body['error']

    def test_missing_state_api_gateway(self, lambda_context):
        """Test API Gateway request fails when state is missing."""
        event = {
            'httpMethod': 'POST',
            'body': json.dumps({'city': 'São Paulo', 'niche': 'Technology'}),
        }

        response = city_collector(event, lambda_context)

        assert response['statusCode'] == 400
        body = json.loads(response['body'])
        assert 'error' in body
        assert 'state' in body['error']

    def test_missing_niche_api_gateway(self, lambda_context):
        """Test API Gateway request fails when niche is missing."""
        event = {
            'httpMethod': 'POST',
            'body': json.dumps({'city': 'São Paulo', 'state': 'SP'}),
        }

        response = city_collector(event, lambda_context)

        assert response['statusCode'] == 400
        body = json.loads(response['body'])
        assert 'error' in body
        assert 'niche' in body['error']

    def test_invalid_state_name(self, lambda_context, valid_payload, mock_env_vars):
        """Test handling of invalid state name."""
        with patch('requests.get') as mock_get:
            mock_response = Mock()
            mock_response.json.return_value = {'response_code': HTTPStatus.NOT_FOUND}
            mock_get.return_value = mock_response

            event = {'httpMethod': 'POST', 'body': json.dumps(valid_payload)}

            response = city_collector(event, lambda_context)

            assert response['statusCode'] == HTTPStatus.NOT_FOUND
            body = json.loads(response['body'])
            assert 'error' in body
            assert 'State name not found' in body['error']

    def test_city_not_in_state(self, lambda_context, mock_env_vars):
        """Test handling when city is not found in the specified state."""
        payload = {'city': 'Curitiba', 'state': 'SP', 'niche': 'Technology'}

        with patch('requests.get') as mock_get:
            # Mock state validation success
            mock_state_response = Mock()
            mock_state_response.json.return_value = {
                'id': 35,
                'sigla': 'SP',
                'nome': 'São Paulo',
            }

            # Mock city list without the requested city
            mock_city_response = Mock()
            mock_city_response.json.return_value = [
                {'codigo_ibge': '3550308', 'nome': 'São Paulo'},
                {'codigo_ibge': '3509502', 'nome': 'Campinas'},
            ]

            mock_get.side_effect = [mock_state_response, mock_city_response]

            event = {'httpMethod': 'POST', 'body': json.dumps(payload)}

            response = city_collector(event, lambda_context)

            assert response['statusCode'] == HTTPStatus.NOT_FOUND
            body = json.loads(response['body'])
            assert 'error' in body
            assert 'City name not found' in body['error']

    def test_exception_handling_api_gateway(self, lambda_context, valid_payload):
        """Test exception handling for API Gateway events."""
        with patch('requests.get', side_effect=Exception('Network error')):
            event = {'httpMethod': 'POST', 'body': json.dumps(valid_payload)}

            response = city_collector(event, lambda_context)

            assert response['statusCode'] == 500
            body = json.loads(response['body'])
            assert 'error' in body
            assert body['error'] == 'Internal Server Error'
            assert 'details' in body

    def test_exception_handling_eventbridge(self, lambda_context, valid_payload):
        """Test exception handling for EventBridge events."""
        with patch('requests.get', side_effect=Exception('Network error')):
            event = {'source': 'aws.events', 'detail': valid_payload}

            response = city_collector(event, lambda_context)

            assert response['success'] is False
            assert 'error' in response
            assert response['error']['error'] == 'Internal Server Error'


# Integration-style tests
class TestCityCollectorIntegration:
    """Integration tests for city_collector function."""

    def test_sqs_message_format(
        self,
        lambda_context,
        valid_payload,
        mock_brasil_api_success,
        mock_sqs_client,
        mock_env_vars,
    ):
        """Test that SQS message is formatted correctly."""
        event = {'httpMethod': 'POST', 'body': json.dumps(valid_payload)}

        city_collector(event, lambda_context)

        # Verify SQS send_message was called with correct parameters
        call_args = mock_sqs_client.send_message.call_args
        assert (
            call_args[1]['QueueUrl']
            == 'https://sqs.us-east-1.amazonaws.com/123456789012/scraper-queue'
        )

        message_body = json.loads(call_args[1]['MessageBody'])
        assert message_body['city'] == valid_payload['city'].upper()
        assert message_body['state'] == valid_payload['state'].upper()
        assert message_body['niche'] == valid_payload['niche'].upper()

    def test_brasil_api_called_correctly(
        self,
        lambda_context,
        valid_payload,
        mock_brasil_api_success,
        mock_sqs_client,
        mock_env_vars,
    ):
        """Test that Brasil API is called with correct parameters."""
        event = {'httpMethod': 'POST', 'body': json.dumps(valid_payload)}

        city_collector(event, lambda_context)

        # Verify requests.get was called twice (state and city validation)
        assert mock_brasil_api_success.call_count == 2

        # Check first call (state validation)
        first_call_url = mock_brasil_api_success.call_args_list[0][0][0]
        assert f"/api/ibge/uf/v1/{valid_payload['state'].upper()}" in first_call_url

        # Check second call (city validation)
        second_call_url = mock_brasil_api_success.call_args_list[1][0][0]
        assert (
            f"/api/ibge/municipios/v1/{valid_payload['state'].upper()}"
            in second_call_url
        )
