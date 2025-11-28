"""Test cases for the data_scrapper handler.

This test file validates the data scrapper handler functionality including:
- SQS event processing
- Message validation
- Scrapper instantiation
- Success and error handling
"""

import json
import os
import sys
from unittest.mock import MagicMock, Mock, patch

import pytest

# Add src directory to the path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from src.functions.data_scrapper.handler import _load_niches, data_scrapper


# Fixtures
@pytest.fixture
def mock_env_vars(monkeypatch):
    """Mock environment variables for settings."""
    monkeypatch.setenv('STAGE', 'dev')
    monkeypatch.setenv('REGION', 'us-east-1')
    monkeypatch.setenv('GOOGLE_PLACES_API_KEY_DEV', 'test-api-key')
    monkeypatch.setenv('GOOGLE_PLACES_DAILY_QUOTA_LIMIT_DEV', '10000')
    monkeypatch.setenv('COMPANIES_TABLE', 'test-companies')
    monkeypatch.setenv('PLACES_TABLE', 'test-places')


@pytest.fixture
def valid_sqs_event():
    """Valid SQS event with all required fields."""
    return {
        'Records': [
            {
                'messageId': 'test-message-id-123',
                'body': json.dumps(
                    {'city': 'SÃO PAULO', 'state': 'SP', 'niche': 'aasi'}
                ),
            }
        ]
    }


@pytest.fixture
def mock_scrapper():
    """Mock GMapsScrapper instance."""
    scrapper = MagicMock()
    scrapper.ensamble = {
        'status': 'completed',
        'status_reason': 'Successfully completed',
        'places': [{'place_id': 'test1'}, {'place_id': 'test2'}],
        'quota_used': 100,
        'stats': {
            'text_searches': 3,
            'details_fetched': 2,
            'new_places': 2,
            'updated_places': 0,
            'skipped_places': 0,
            'duplicates_by_place_id': 0,
            'duplicates_by_location': 0,
        },
    }
    scrapper.collect_data = MagicMock()
    return scrapper


@pytest.fixture
def mock_context():
    """Mock Lambda context."""
    context = MagicMock()
    context.function_name = 'test-function'
    context.invoked_function_arn = 'arn:aws:lambda:us-east-1:123456789:function:test'
    return context


# Tests
class TestDataScrapperHandler:
    """Tests for the data_scrapper handler function."""

    @patch('src.functions.data_scrapper.handler.GMapsScrapper')
    def test_successful_scraping(
        self,
        mock_scrapper_class,
        valid_sqs_event,
        mock_context,
        mock_env_vars,
        mock_scrapper,
    ):
        """Test successful data scraping with valid SQS event."""
        mock_scrapper_class.return_value = mock_scrapper

        result = data_scrapper(valid_sqs_event, mock_context)

        # Verify scrapper was instantiated
        mock_scrapper_class.assert_called_once_with(
            niche='aasi', api_key='test-api-key', daily_quota_limit=10000
        )

        # Verify collect_data was called
        mock_scrapper.collect_data.assert_called_once_with(
            city='SÃO PAULO', state='SP'
        )

        # Verify response
        assert result['statusCode'] == 200
        body = json.loads(result['body'])
        assert body['success'] is True
        assert body['city'] == 'SÃO PAULO'
        assert body['state'] == 'SP'
        assert body['niche'] == 'aasi'
        assert body['places_collected'] == 2
        assert body['quota_used'] == 100

    def test_missing_records_in_event(self, mock_context, mock_env_vars):
        """Test error handling when no Records in event."""
        event = {}

        result = data_scrapper(event, mock_context)

        assert result['statusCode'] == 500
        body = json.loads(result['body'])
        assert body['success'] is False
        assert 'No SQS records' in body['details']

    def test_missing_city_field(self, mock_context, mock_env_vars):
        """Test error handling when city is missing."""
        event = {
            'Records': [
                {
                    'messageId': 'test-id',
                    'body': json.dumps({'state': 'SP', 'niche': 'aasi'}),
                }
            ]
        }

        result = data_scrapper(event, mock_context)

        assert result['statusCode'] == 500
        body = json.loads(result['body'])
        assert body['success'] is False
        assert 'Missing required fields' in body['details']

    def test_missing_state_field(self, mock_context, mock_env_vars):
        """Test error handling when state is missing."""
        event = {
            'Records': [
                {
                    'messageId': 'test-id',
                    'body': json.dumps({'city': 'SÃO PAULO', 'niche': 'aasi'}),
                }
            ]
        }

        result = data_scrapper(event, mock_context)

        assert result['statusCode'] == 500
        body = json.loads(result['body'])
        assert body['success'] is False
        assert 'Missing required fields' in body['details']

    @patch('src.functions.data_scrapper.handler.GMapsScrapper')
    def test_default_niche_when_not_provided(
        self, mock_scrapper_class, mock_context, mock_env_vars, mock_scrapper
    ):
        """Test that default niche 'aasi' is used when not provided."""
        mock_scrapper_class.return_value = mock_scrapper
        event = {
            'Records': [
                {
                    'messageId': 'test-id',
                    'body': json.dumps({'city': 'SÃO PAULO', 'state': 'SP'}),
                }
            ]
        }

        result = data_scrapper(event, mock_context)

        assert result['statusCode'] == 200
        body = json.loads(result['body'])
        assert body['niche'] == 'aasi'

    def test_invalid_niche(self, mock_context, mock_env_vars):
        """Test error handling with invalid niche."""
        event = {
            'Records': [
                {
                    'messageId': 'test-id',
                    'body': json.dumps(
                        {'city': 'SÃO PAULO', 'state': 'SP', 'niche': 'invalid_niche'}
                    ),
                }
            ]
        }

        result = data_scrapper(event, mock_context)

        assert result['statusCode'] == 500
        body = json.loads(result['body'])
        assert body['success'] is False
        assert 'Unknown niche' in body['details']

    @patch('src.functions.data_scrapper.handler.GMapsScrapper')
    def test_scraping_failed_status(
        self, mock_scrapper_class, valid_sqs_event, mock_context, mock_env_vars
    ):
        """Test error handling when scraping returns failed status."""
        failed_scrapper = MagicMock()
        failed_scrapper.ensamble = {
            'status': 'failed_api_error',
            'status_reason': 'API request failed',
            'places': [],
            'quota_used': 50,
            'stats': {},
        }
        mock_scrapper_class.return_value = failed_scrapper

        result = data_scrapper(valid_sqs_event, mock_context)

        assert result['statusCode'] == 500
        body = json.loads(result['body'])
        assert body['success'] is False
        assert 'Scraping failed' in body['details']

    @patch('src.functions.data_scrapper.handler.GMapsScrapper')
    def test_body_as_dict_instead_of_string(
        self, mock_scrapper_class, mock_context, mock_env_vars, mock_scrapper
    ):
        """Test handling when body is already a dict instead of JSON string."""
        mock_scrapper_class.return_value = mock_scrapper
        event = {
            'Records': [
                {
                    'messageId': 'test-id',
                    'body': {'city': 'SÃO PAULO', 'state': 'SP', 'niche': 'aasi'},
                }
            ]
        }

        result = data_scrapper(event, mock_context)

        assert result['statusCode'] == 200
        body = json.loads(result['body'])
        assert body['success'] is True


class TestLoadNiches:
    """Tests for the _load_niches helper function."""

    @patch('builtins.open')
    def test_load_niches_success(self, mock_open):
        """Test successful loading of niches from JSON file."""
        mock_file = MagicMock()
        mock_file.__enter__.return_value.read.return_value = json.dumps(
            {'aasi': ['term1'], 'orl': ['term2'], 'geria': ['term3']}
        )
        mock_open.return_value = mock_file

        # Mock json.load to return test data
        with patch('json.load') as mock_json_load:
            mock_json_load.return_value = {
                'aasi': ['term1'],
                'orl': ['term2'],
                'geria': ['term3'],
            }
            niches = _load_niches()

        assert 'aasi' in niches
        assert 'orl' in niches
        assert 'geria' in niches
        assert len(niches) == 3

    @patch('builtins.open', side_effect=FileNotFoundError())
    def test_load_niches_file_not_found(self, mock_open):
        """Test fallback to default niche when file not found."""
        niches = _load_niches()

        assert niches == ['aasi']

    @patch('builtins.open')
    @patch('json.load', side_effect=json.JSONDecodeError('test', 'test', 0))
    def test_load_niches_json_decode_error(self, mock_json_load, mock_open):
        """Test fallback to default niche on JSON decode error."""
        niches = _load_niches()

        assert niches == ['aasi']
