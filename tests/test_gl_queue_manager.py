"""Test cases for the gl_queue_manager handler.

This test file validates the queue manager handler functionality including:
- SQS event processing
- operationType validation
- Message attribute enrichment (userEmail, companyID)
- Routing to appropriate target queues
- Error handling for invalid operations
"""

import json
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

# Add src directory to the path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from src.functions.gl_queue_manager.gl_queue_manager_handler import gl_queue_manager


# Fixtures
@pytest.fixture
def mock_env_vars(monkeypatch):
    """Mock environment variables for settings."""
    monkeypatch.setenv('STAGE', 'dev')
    monkeypatch.setenv('REGION', 'us-east-1')
    monkeypatch.setenv('AWS_ACCOUNT_ID', '819774487459')
    monkeypatch.setenv(
        'OPERATIONS_QUEUE_URL',
        'https://sqs.us-east-1.amazonaws.com/819774487459/backend-core-dev-gl-operations-queue',
    )
    monkeypatch.setenv(
        'WEBSITE_SCRAPER_TASK_QUEUE_URL',
        'https://sqs.us-east-1.amazonaws.com/819774487459/backend-core-dev-website-scraper-tasks',
    )
    monkeypatch.setenv(
        'COMPANY_FEDERAL_SCRAPER_TASK_QUEUE_URL',
        'https://sqs.us-east-1.amazonaws.com/819774487459/backend-core-dev-company-federal-scraper-tasks',
    )
    monkeypatch.setenv(
        'SCRAPER_TASK_QUEUE_URL',
        'https://sqs.us-east-1.amazonaws.com/819774487459/backend-core-dev-scraper-tasks',
    )
    monkeypatch.setenv('AWS_REGION_NAME', 'us-east-1')


@pytest.fixture
def mock_context():
    """Mock Lambda context."""
    context = MagicMock()
    context.function_name = 'gl_queue_manager'
    context.invoked_function_arn = (
        'arn:aws:lambda:us-east-1:819774487459:function:gl_queue_manager'
    )
    return context


@pytest.fixture
def valid_sqs_event_communication_registration():
    """Valid SQS event for communication_registration operation."""
    return {
        'Records': [
            {
                'messageId': 'test-message-id-001',
                'receiptHandle': 'test-receipt-handle-001',
                'body': json.dumps(
                    {
                        'operationType': 'add_new_lead',
                        'payload': {
                            'leadID': 'lead-123',
                            'message': 'Follow-up call completed',
                        },
                        'timestamp': '2026-01-21T10:00:00.000Z',
                        'userEmail': 'user@example.com',
                        'companyID': 'company-123',
                        'invocationType': 'RequestResponse',
                    }
                ),
            }
        ]
    }


@pytest.fixture
def valid_sqs_event_multiple_records():
    """Valid SQS event with lead update operation."""
    return {
        'Records': [
            {
                'messageId': 'test-message-id-002',
                'receiptHandle': 'test-receipt-handle-002',
                'body': json.dumps(
                    {
                        'operationType': 'add_new_lead',
                        'payload': {'leadID': 'lead-456', 'message': 'Status updated'},
                        'timestamp': '2026-01-21T10:00:00.000Z',
                        'userEmail': 'admin@example.com',
                        'companyID': 'company-456',
                    }
                ),
            }
        ]
    }


@pytest.fixture
def valid_sqs_event_with_optional_fields():
    """Valid SQS event with optional status field."""
    return {
        'Records': [
            {
                'messageId': 'test-message-id-003',
                'receiptHandle': 'test-receipt-handle-003',
                'body': json.dumps(
                    {
                        'operationType': 'add_new_lead',
                        'payload': {
                            'leadID': 'lead-789',
                            'message': 'Initial contact',
                            'status': 'Em contato',
                        },
                        'timestamp': '2026-01-21T10:00:00.000Z',
                        'userEmail': 'user@example.com',
                        'companyID': 'company-789',
                    }
                ),
            }
        ]
    }


@pytest.fixture
def invalid_operation_type_event():
    """SQS event with invalid operationType."""
    return {
        'Records': [
            {
                'messageId': 'test-message-id-bad',
                'receiptHandle': 'test-receipt-handle-bad',
                'body': json.dumps(
                    {
                        'operationType': 'INVALID_OPERATION',
                        'payload': {'leadID': 'lead-123', 'message': 'test'},
                        'timestamp': '2026-01-21T10:00:00.000Z',
                        'userEmail': 'user@example.com',
                        'companyID': 'company-000',
                    }
                ),
            }
        ]
    }


@pytest.fixture
def missing_payload_event():
    """SQS event missing payload field."""
    return {
        'Records': [
            {
                'messageId': 'test-message-id-missing',
                'receiptHandle': 'test-receipt-handle-missing',
                'body': json.dumps(
                    {
                        'operationType': 'add_new_lead',
                        'timestamp': '2026-01-21T10:00:00.000Z',
                        'userEmail': 'user@example.com',
                        'companyID': 'company-000',
                    }
                ),
            }
        ]
    }


@pytest.fixture
def missing_operation_type_event():
    """SQS event missing operationType field."""
    return {
        'Records': [
            {
                'messageId': 'test-message-id-no-type',
                'receiptHandle': 'test-receipt-handle-no-type',
                'body': json.dumps(
                    {
                        'payload': {'leadID': 'lead-123', 'message': 'test'},
                        'timestamp': '2026-01-21T10:00:00.000Z',
                        'userEmail': 'user@example.com',
                        'companyID': 'company-000',
                    }
                ),
            }
        ]
    }


@pytest.fixture
def empty_records_event():
    """SQS event with empty Records."""
    return {'Records': []}


# Test Cases
class TestGlQueueManagerValidOperations:
    """Test valid operation routing."""

    @patch('boto3.client')
    def test_communication_registration_operation_success(
        self,
        mock_boto_client,
        mock_env_vars,
        mock_context,
        valid_sqs_event_communication_registration,
    ):
        """Test successful routing of communication_registration operation."""
        # Setup mock Lambda client
        mock_lambda = MagicMock()
        mock_boto_client.return_value = mock_lambda

        # Mock Lambda response
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps(
            {'message': 'Lead created'}
        ).encode()
        mock_lambda.invoke.return_value = {'StatusCode': 200, 'Payload': mock_response}

        # Execute handler
        response = gl_queue_manager(
            valid_sqs_event_communication_registration, mock_context
        )

        # Assertions
        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert body['message'] == 'Operation executed successfully'
        assert body['operationType'] == 'add_new_lead'

        # Verify Lambda invoke was called with correct params
        mock_lambda.invoke.assert_called_once()
        call_args = mock_lambda.invoke.call_args
        assert call_args.kwargs['FunctionName'] == 'gl-add-new-lead'
        assert call_args.kwargs['InvocationType'] == 'RequestResponse'

        # Verify enriched payload structure
        payload = json.loads(call_args.kwargs['Payload'])
        assert 'body' in payload
        assert payload['body']['userEmail'] == 'user@example.com'
        assert payload['body']['companyID'] == 'company-123'
        assert payload['body']['operationType'] == 'add_new_lead'

    @patch('boto3.client')
    def test_multiple_operations_success(
        self,
        mock_boto_client,
        mock_env_vars,
        mock_context,
        valid_sqs_event_multiple_records,
    ):
        """Test successful routing of multiple operation types."""
        # Setup mock Lambda client
        mock_lambda = MagicMock()
        mock_boto_client.return_value = mock_lambda

        # Mock Lambda response
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({'status': 'processed'}).encode()
        mock_lambda.invoke.return_value = {'StatusCode': 200, 'Payload': mock_response}

        # Execute handler
        response = gl_queue_manager(valid_sqs_event_multiple_records, mock_context)

        # Assertions
        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert body['operationType'] == 'add_new_lead'

        # Verify correct Lambda function was invoked
        call_args = mock_lambda.invoke.call_args
        assert call_args.kwargs['FunctionName'] == 'gl-add-new-lead'

    @patch('boto3.client')
    def test_operation_with_optional_fields(
        self,
        mock_boto_client,
        mock_env_vars,
        mock_context,
        valid_sqs_event_with_optional_fields,
    ):
        """Test successful routing with optional payload fields."""
        # Setup mock Lambda client
        mock_lambda = MagicMock()
        mock_boto_client.return_value = mock_lambda

        # Mock Lambda response
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({'status': 'processed'}).encode()
        mock_lambda.invoke.return_value = {'StatusCode': 200, 'Payload': mock_response}

        # Execute handler
        response = gl_queue_manager(valid_sqs_event_with_optional_fields, mock_context)

        # Assertions
        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert body['operationType'] == 'add_new_lead'

        # Verify optional fields were passed in payload
        call_args = mock_lambda.invoke.call_args
        payload = json.loads(call_args.kwargs['Payload'])
        assert 'status' in payload['body']

    @patch('boto3.client')
    def test_enriched_payload_structure(
        self,
        mock_boto_client,
        mock_env_vars,
        mock_context,
        valid_sqs_event_communication_registration,
    ):
        """Test that enriched payload is properly constructed."""
        # Setup mock Lambda client
        mock_lambda = MagicMock()
        mock_boto_client.return_value = mock_lambda

        # Mock Lambda response
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({'result': 'success'}).encode()
        mock_lambda.invoke.return_value = {'StatusCode': 200, 'Payload': mock_response}

        # Execute handler
        gl_queue_manager(valid_sqs_event_communication_registration, mock_context)

        # Extract invocation payload from the call
        call_args = mock_lambda.invoke.call_args
        payload = json.loads(call_args.kwargs['Payload'])

        # Verify enrichment structure
        assert 'body' in payload
        assert payload['body']['userEmail'] == 'user@example.com'
        assert payload['body']['companyID'] == 'company-123'
        assert payload['body']['operationType'] == 'add_new_lead'


class TestGlQueueManagerErrorHandling:
    """Test error handling and validation."""

    def test_missing_payload_error(
        self, mock_env_vars, mock_context, missing_payload_event
    ):
        """Test error response when payload is missing."""
        response = gl_queue_manager(missing_payload_event, mock_context)

        assert response['statusCode'] == 400
        body = json.loads(response['body'])
        assert 'payload' in body['error'].lower()

    def test_missing_operation_type_error(
        self, mock_env_vars, mock_context, missing_operation_type_event
    ):
        """Test error response when operationType is missing."""
        response = gl_queue_manager(missing_operation_type_event, mock_context)

        assert response['statusCode'] == 400
        body = json.loads(response['body'])
        assert 'operationType' in body['error']

    def test_empty_records_error(
        self, mock_env_vars, mock_context, empty_records_event
    ):
        """Test error response when no records are provided."""
        response = gl_queue_manager(empty_records_event, mock_context)

        assert response['statusCode'] == 400
        body = json.loads(response['body'])
        assert 'No SQS records found' in body['error']

    def test_invalid_json_in_body_error(self, mock_env_vars, mock_context):
        """Test error response for invalid JSON in message body."""
        event = {
            'Records': [
                {
                    'messageId': 'test-bad-json',
                    'receiptHandle': 'test-receipt',
                    'body': 'invalid json {not: valid}',
                }
            ]
        }

        response = gl_queue_manager(event, mock_context)

        assert response['statusCode'] == 400
        body = json.loads(response['body'])
        assert 'Invalid JSON' in body['error']

    @patch('boto3.client')
    def test_lambda_invoke_error_handling(
        self,
        mock_boto_client,
        mock_env_vars,
        mock_context,
        valid_sqs_event_communication_registration,
    ):
        """Test error handling when Lambda invoke fails."""
        # Setup mock Lambda client to raise exception
        mock_lambda = MagicMock()
        mock_boto_client.return_value = mock_lambda
        mock_lambda.invoke.side_effect = Exception('Lambda invoke failed')

        # Execute handler
        response = gl_queue_manager(
            valid_sqs_event_communication_registration, mock_context
        )

        # Should return error response with 500 status
        assert response['statusCode'] == 500
        body = json.loads(response['body'])
        assert 'error' in body


class TestGlQueueManagerEdgeCases:
    """Test edge cases and boundary conditions."""

    @patch('boto3.client')
    def test_empty_user_email_and_company_id(
        self, mock_boto_client, mock_env_vars, mock_context
    ):
        """Test handling of empty userEmail and companyID."""
        event = {
            'Records': [
                {
                    'messageId': 'test-empty-attrs',
                    'receiptHandle': 'test-receipt',
                    'body': json.dumps(
                        {
                            'operationType': 'add_new_lead',
                            'payload': {'leadID': 'lead-123', 'message': 'test'},
                            'timestamp': '2026-01-21T10:00:00.000Z',
                            'userEmail': '',
                            'companyID': '',
                            'invocationType': 'RequestResponse',
                        }
                    ),
                }
            ]
        }

        # Setup mock Lambda client
        mock_lambda = MagicMock()
        mock_boto_client.return_value = mock_lambda
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({'status': 'ok'}).encode()
        mock_lambda.invoke.return_value = {'StatusCode': 200, 'Payload': mock_response}

        # Execute handler
        response = gl_queue_manager(event, mock_context)

        # Should succeed even with empty attributes
        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert body['message'] == 'Operation executed successfully'

    @patch('boto3.client')
    def test_case_insensitive_operation_type(
        self, mock_boto_client, mock_env_vars, mock_context
    ):
        """Test that operationType is case-insensitive."""
        event = {
            'Records': [
                {
                    'messageId': 'test-case-insensitive',
                    'receiptHandle': 'test-receipt',
                    'body': json.dumps(
                        {
                            'operationType': 'add_new_lead',  # uppercase - will be normalized
                            'payload': {'leadID': 'lead-123', 'message': 'test'},
                            'timestamp': '2026-01-21T10:00:00.000Z',
                            'userEmail': 'user@example.com',
                            'companyID': 'company-123',
                            'invocationType': 'RequestResponse',
                        }
                    ),
                }
            ]
        }

        # Setup mock Lambda client
        mock_lambda = MagicMock()
        mock_boto_client.return_value = mock_lambda
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({'status': 'ok'}).encode()
        mock_lambda.invoke.return_value = {'StatusCode': 200, 'Payload': mock_response}

        # Execute handler
        response = gl_queue_manager(event, mock_context)

        # Should succeed with normalized operation type
        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert body['operationType'] == 'add_new_lead'
