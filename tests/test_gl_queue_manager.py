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

from src.functions.gl_queue_manager.gl_queue_manager_handler import (
    gl_queue_manager,
)


# Fixtures
@pytest.fixture
def mock_env_vars(monkeypatch):
    """Mock environment variables for settings."""
    monkeypatch.setenv('STAGE', 'dev')
    monkeypatch.setenv('REGION', 'us-east-1')
    monkeypatch.setenv('ACCOUNT_ID', '819774487459')
    monkeypatch.setenv('OPERATIONS_QUEUE_URL', 'https://sqs.us-east-1.amazonaws.com/819774487459/backend-core-dev-gl-operations-queue')
    monkeypatch.setenv('WEBSITE_SCRAPER_TASK_QUEUE_URL', 'https://sqs.us-east-1.amazonaws.com/819774487459/backend-core-dev-website-scraper-tasks')
    monkeypatch.setenv('COMPANY_FEDERAL_SCRAPER_TASK_QUEUE_URL', 'https://sqs.us-east-1.amazonaws.com/819774487459/backend-core-dev-company-federal-scraper-tasks')
    monkeypatch.setenv('SCRAPER_TASK_QUEUE_URL', 'https://sqs.us-east-1.amazonaws.com/819774487459/backend-core-dev-scraper-tasks')
    monkeypatch.setenv('AWS_REGION_NAME', 'us-east-1')


@pytest.fixture
def mock_context():
    """Mock Lambda context."""
    context = MagicMock()
    context.function_name = 'gl_queue_manager'
    context.invoked_function_arn = 'arn:aws:lambda:us-east-1:819774487459:function:gl_queue_manager'
    return context


@pytest.fixture
def valid_sqs_event_website_scraper():
    """Valid SQS event for WEBSITE_SCRAPER operation."""
    return {
        'Records': [
            {
                'messageId': 'test-message-id-001',
                'receiptHandle': 'test-receipt-handle-001',
                'body': json.dumps({
                    'operationType': 'WEBSITE_SCRAPER',
                    'payload': {
                        'company_id': '123',
                        'website': 'https://example.com'
                    },
                    'timestamp': '2026-01-21T10:00:00.000Z',
                    'userEmail': 'user@example.com',
                    'companyID': 'company-123'
                })
            }
        ]
    }


@pytest.fixture
def valid_sqs_event_federal_scraper():
    """Valid SQS event for FEDERAL_SCRAPER operation."""
    return {
        'Records': [
            {
                'messageId': 'test-message-id-002',
                'receiptHandle': 'test-receipt-handle-002',
                'body': json.dumps({
                    'operationType': 'FEDERAL_SCRAPER',
                    'payload': {
                        'cnpj': '12345678000190'
                    },
                    'timestamp': '2026-01-21T10:00:00.000Z',
                    'userEmail': 'admin@example.com',
                    'companyID': 'company-456'
                })
            }
        ]
    }


@pytest.fixture
def valid_sqs_event_scraper_task():
    """Valid SQS event for SCRAPER_TASK operation."""
    return {
        'Records': [
            {
                'messageId': 'test-message-id-003',
                'receiptHandle': 'test-receipt-handle-003',
                'body': json.dumps({
                    'operationType': 'SCRAPER_TASK',
                    'payload': {
                        'city': 'SÃO PAULO',
                        'state': 'SP'
                    },
                    'timestamp': '2026-01-21T10:00:00.000Z',
                    'userEmail': 'user@example.com',
                    'companyID': 'company-789'
                })
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
                'body': json.dumps({
                    'operationType': 'INVALID_OPERATION',
                    'payload': {'some': 'data'},
                    'timestamp': '2026-01-21T10:00:00.000Z',
                    'userEmail': 'user@example.com',
                    'companyID': 'company-000'
                })
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
                'body': json.dumps({
                    'operationType': 'WEBSITE_SCRAPER',
                    'timestamp': '2026-01-21T10:00:00.000Z',
                    'userEmail': 'user@example.com',
                    'companyID': 'company-000'
                })
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
                'body': json.dumps({
                    'payload': {'some': 'data'},
                    'timestamp': '2026-01-21T10:00:00.000Z',
                    'userEmail': 'user@example.com',
                    'companyID': 'company-000'
                })
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
    def test_website_scraper_operation_success(
        self, mock_boto_client, mock_env_vars, mock_context, valid_sqs_event_website_scraper
    ):
        """Test successful routing of WEBSITE_SCRAPER operation."""
        # Setup mock SQS client
        mock_sqs = MagicMock()
        mock_boto_client.return_value = mock_sqs
        mock_sqs.send_message.return_value = {'MessageId': 'msg-id-001'}

        # Execute handler
        response = gl_queue_manager(valid_sqs_event_website_scraper, mock_context)

        # Assertions
        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert body['message'] == 'Operation queued successfully'
        assert body['operationType'] == 'WEBSITE_SCRAPER'
        assert body['messageId'] == 'msg-id-001'

        # Verify SQS send_message was called with correct params
        mock_sqs.send_message.assert_called_once()
        call_args = mock_sqs.send_message.call_args
        assert 'MessageAttributes' in call_args.kwargs
        assert call_args.kwargs['MessageAttributes']['userEmail']['StringValue'] == 'user@example.com'
        assert call_args.kwargs['MessageAttributes']['companyID']['StringValue'] == 'company-123'
        assert call_args.kwargs['MessageAttributes']['operationType']['StringValue'] == 'WEBSITE_SCRAPER'

    @patch('boto3.client')
    def test_federal_scraper_operation_success(
        self, mock_boto_client, mock_env_vars, mock_context, valid_sqs_event_federal_scraper
    ):
        """Test successful routing of FEDERAL_SCRAPER operation."""
        # Setup mock SQS client
        mock_sqs = MagicMock()
        mock_boto_client.return_value = mock_sqs
        mock_sqs.send_message.return_value = {'MessageId': 'msg-id-002'}

        # Execute handler
        response = gl_queue_manager(valid_sqs_event_federal_scraper, mock_context)

        # Assertions
        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert body['operationType'] == 'FEDERAL_SCRAPER'

        # Verify correct queue URL was used
        call_args = mock_sqs.send_message.call_args
        assert 'company-federal-scraper-tasks' in call_args.kwargs['QueueUrl']

    @patch('boto3.client')
    def test_scraper_task_operation_success(
        self, mock_boto_client, mock_env_vars, mock_context, valid_sqs_event_scraper_task
    ):
        """Test successful routing of SCRAPER_TASK operation."""
        # Setup mock SQS client
        mock_sqs = MagicMock()
        mock_boto_client.return_value = mock_sqs
        mock_sqs.send_message.return_value = {'MessageId': 'msg-id-003'}

        # Execute handler
        response = gl_queue_manager(valid_sqs_event_scraper_task, mock_context)

        # Assertions
        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert body['operationType'] == 'SCRAPER_TASK'

    @patch('boto3.client')
    def test_message_attributes_enrichment(
        self, mock_boto_client, mock_env_vars, mock_context, valid_sqs_event_website_scraper
    ):
        """Test that message attributes are properly enriched."""
        # Setup mock SQS client
        mock_sqs = MagicMock()
        mock_boto_client.return_value = mock_sqs
        mock_sqs.send_message.return_value = {'MessageId': 'msg-id-enriched'}

        # Execute handler
        gl_queue_manager(valid_sqs_event_website_scraper, mock_context)

        # Extract message attributes from the call
        call_args = mock_sqs.send_message.call_args
        attrs = call_args.kwargs['MessageAttributes']

        # Verify enrichment
        assert attrs['userEmail']['StringValue'] == 'user@example.com'
        assert attrs['userEmail']['DataType'] == 'String'
        assert attrs['companyID']['StringValue'] == 'company-123'
        assert attrs['companyID']['DataType'] == 'String'
        assert attrs['operationType']['StringValue'] == 'WEBSITE_SCRAPER'
        assert attrs['operationType']['DataType'] == 'String'


class TestGlQueueManagerErrorHandling:
    """Test error handling and validation."""

    def test_invalid_operation_type_error(
        self, mock_env_vars, mock_context, invalid_operation_type_event
    ):
        """Test error response for invalid operationType."""
        response = gl_queue_manager(invalid_operation_type_event, mock_context)

        assert response['statusCode'] == 400
        body = json.loads(response['body'])
        assert 'Invalid operationType' in body['error']
        assert 'INVALID_OPERATION' in body['error']

    def test_missing_payload_error(
        self, mock_env_vars, mock_context, missing_payload_event
    ):
        """Test error response when payload is missing."""
        response = gl_queue_manager(missing_payload_event, mock_context)

        assert response['statusCode'] == 400
        body = json.loads(response['body'])
        assert 'payload' in body['error']

    def test_missing_operation_type_error(
        self, mock_env_vars, mock_context, missing_operation_type_event
    ):
        """Test error response when operationType is missing."""
        response = gl_queue_manager(missing_operation_type_event, mock_context)

        assert response['statusCode'] == 400
        body = json.loads(response['body'])
        assert 'operationType' in body['error']

    def test_empty_records_error(self, mock_env_vars, mock_context, empty_records_event):
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
                    'body': 'invalid json {not: valid}'
                }
            ]
        }

        response = gl_queue_manager(event, mock_context)

        assert response['statusCode'] == 400
        body = json.loads(response['body'])
        assert 'Invalid JSON' in body['error']

    @patch('boto3.client')
    def test_sqs_send_error_handling(
        self, mock_boto_client, mock_env_vars, mock_context, valid_sqs_event_website_scraper
    ):
        """Test error handling when SQS send fails."""
        # Setup mock SQS client to raise exception
        mock_sqs = MagicMock()
        mock_boto_client.return_value = mock_sqs
        mock_sqs.send_message.side_effect = Exception('SQS send failed')

        # Execute handler
        response = gl_queue_manager(valid_sqs_event_website_scraper, mock_context)

        # Should return error response
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
                    'body': json.dumps({
                        'operationType': 'WEBSITE_SCRAPER',
                        'payload': {'data': 'test'},
                        'timestamp': '2026-01-21T10:00:00.000Z',
                        'userEmail': '',
                        'companyID': ''
                    })
                }
            ]
        }

        # Setup mock SQS client
        mock_sqs = MagicMock()
        mock_boto_client.return_value = mock_sqs
        mock_sqs.send_message.return_value = {'MessageId': 'msg-id-empty'}

        # Execute handler
        response = gl_queue_manager(event, mock_context)

        # Should succeed even with empty attributes
        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert body['message'] == 'Operation queued successfully'

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
                    'body': json.dumps({
                        'operationType': 'website_scraper',  # lowercase
                        'payload': {'data': 'test'},
                        'timestamp': '2026-01-21T10:00:00.000Z',
                        'userEmail': 'user@example.com',
                        'companyID': 'company-123'
                    })
                }
            ]
        }

        # Setup mock SQS client
        mock_sqs = MagicMock()
        mock_boto_client.return_value = mock_sqs
        mock_sqs.send_message.return_value = {'MessageId': 'msg-id-case'}

        # Execute handler
        response = gl_queue_manager(event, mock_context)

        # Should succeed with lowercase operation type
        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert body['operationType'] == 'WEBSITE_SCRAPER'

    def test_empty_payload_object(self, mock_env_vars, mock_context):
        """Test handling of empty payload object (but not None)."""
        event = {
            'Records': [
                {
                    'messageId': 'test-empty-payload-obj',
                    'receiptHandle': 'test-receipt',
                    'body': json.dumps({
                        'operationType': 'WEBSITE_SCRAPER',
                        'payload': {},  # Empty but valid
                        'timestamp': '2026-01-21T10:00:00.000Z',
                        'userEmail': 'user@example.com',
                        'companyID': 'company-123'
                    })
                }
            ]
        }

        # Mock client
        with patch('boto3.client') as mock_boto_client:
            mock_sqs = MagicMock()
            mock_boto_client.return_value = mock_sqs
            mock_sqs.send_message.return_value = {'MessageId': 'msg-id-empty-obj'}

            response = gl_queue_manager(event, mock_context)

            # Should succeed even with empty payload
            assert response['statusCode'] == 200
