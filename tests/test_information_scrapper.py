"""
Test cases for the InformationScrapper class.

This test file validates the InformationScrapper functionality including:
- Constructor initialization
- Google Places API integration (Text Search and Details)
- Deduplication logic (by place_id and geolocation)
- Quota management
- Database persistence
- Error handling
"""

import json
import os
import sys
from unittest.mock import MagicMock, Mock, patch

import pytest

# Add src directory to the path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from src.models.scrappers.information_scrapper import InformationScrapper


# Fixtures
@pytest.fixture
def mock_env_vars(monkeypatch):
    """Mock environment variables."""
    monkeypatch.setenv('STAGE', 'dev')
    monkeypatch.setenv('REGION', 'us-east-1')
    monkeypatch.setenv('GOOGLE_PLACES_API_KEY_DEV', 'test-api-key-12345')
    monkeypatch.setenv('GOOGLE_PLACES_DAILY_QUOTA_LIMIT_DEV', '20000')
    monkeypatch.setenv('COMPANIES_TABLE', 'test-auris-core-companies')
    monkeypatch.setenv('PLACES_TABLE', 'test-auris-core-places')


@pytest.fixture
def valid_api_key():
    """Valid Google Places API key for testing."""
    return 'test-api-key-12345'


@pytest.fixture
def mock_text_search_response():
    """Mock Google Places API (New) Text Search response."""
    return {
        'places': [
            {
                'id': 'ChIJN1t_tDeuEmsRUsoyG83frY4',
                'displayName': {'text': 'Centro Auditivo São Paulo'},
                'formattedAddress': 'Av. Paulista, 1000 - Bela Vista, São Paulo - SP, 01310-100',
                'location': {'latitude': -23.5505199, 'longitude': -46.6333094},
                'rating': 4.5,
                'userRatingCount': 120,
                'businessStatus': 'OPERATIONAL',
                'types': ['store', 'health'],
                'currentOpeningHours': {'openNow': True},
                'photos': [{'name': 'photo-ref-123', 'heightPx': 800, 'widthPx': 1200}],
            },
            {
                'id': 'ChIJN1t_tDeuEmsRUsoyG83frY5',
                'displayName': {'text': 'Loja Aparelhos Auditivos Center'},
                'formattedAddress': 'Rua Augusta, 500 - Consolação, São Paulo - SP, 01305-000',
                'location': {'latitude': -23.5525199, 'longitude': -46.6353094},
                'rating': 4.8,
                'userRatingCount': 85,
                'businessStatus': 'OPERATIONAL',
                'types': ['store', 'health'],
            },
        ],
    }


@pytest.fixture
def mock_text_search_response_with_pagination():
    """Mock paginated Google Places API (New) Text Search response."""
    return {
        'places': [
            {
                'id': 'ChIJN1t_page2_test',
                'displayName': {'text': 'Audicare Premium'},
                'formattedAddress': 'Rua da Consolação, 2000 - Consolação, São Paulo - SP',
                'location': {'latitude': -23.5545199, 'longitude': -46.6373094},
                'rating': 4.7,
                'userRatingCount': 95,
                'businessStatus': 'OPERATIONAL',
                'types': ['store'],
            }
        ],
        'nextPageToken': 'test-next-page-token-123',
    }


@pytest.fixture
def mock_place_details_response():
    """Mock Google Places API (New) Place Details response."""
    return {
        'id': 'ChIJN1t_tDeuEmsRUsoyG83frY4',
        'displayName': {'text': 'Centro Auditivo São Paulo'},
        'formattedAddress': 'Av. Paulista, 1000 - Bela Vista, São Paulo - SP, 01310-100',
        'location': {'latitude': -23.5505199, 'longitude': -46.6333094},
        'nationalPhoneNumber': '(11) 3456-7890',
        'internationalPhoneNumber': '+55 11 3456-7890',
        'websiteUri': 'https://centroauditivo.com.br',
        'googleMapsUri': 'https://maps.google.com/?cid=12345678901234567890',
        'rating': 4.5,
        'userRatingCount': 120,
        'businessStatus': 'OPERATIONAL',
        'types': ['store', 'health'],
        'currentOpeningHours': {
            'openNow': True,
            'periods': [
                {
                    'open': {'day': 1, 'hour': 9, 'minute': 0},
                    'close': {'day': 1, 'hour': 18, 'minute': 0},
                }
            ],
            'weekdayDescriptions': [
                'Monday: 9:00 AM – 6:00 PM',
                'Tuesday: 9:00 AM – 6:00 PM',
            ],
        },
        'reviews': [
            {
                'authorAttribution': {'displayName': 'João Silva'},
                'rating': 5,
                'text': {'text': 'Excelente atendimento!'},
                'publishTime': '2021-10-18T12:34:50Z',
            }
        ],
        'priceLevel': 'PRICE_LEVEL_MODERATE',
        'photos': [{'name': 'photo-ref-123', 'heightPx': 800, 'widthPx': 1200}],
    }


@pytest.fixture
def mock_database_handler(mock_env_vars):
    """Mock DatabaseHandler."""
    with patch('src.models.scrappers.information_scrapper.DatabaseHandler') as mock_db:
        mock_db_instance = MagicMock()

        # Mock get_item to return None (place doesn't exist)
        mock_db_instance.get_item.return_value = None

        # Mock put_item to succeed
        mock_db_instance.put_item.return_value = {
            'ResponseMetadata': {'HTTPStatusCode': 200}
        }

        # Mock update_item to succeed
        mock_db_instance.update_item.return_value = {
            'ResponseMetadata': {'HTTPStatusCode': 200}
        }

        mock_db.return_value = mock_db_instance
        yield mock_db_instance


# Tests for Constructor
class TestInformationScrapperConstructor:
    """Tests for InformationScrapper constructor initialization."""

    def test_constructor_with_valid_parameters(
        self, valid_api_key, mock_database_handler
    ):
        """Test that constructor initializes correctly with valid parameters."""
        scrapper = InformationScrapper(
            niche='aasi', api_key=valid_api_key, daily_quota_limit=20000
        )

        assert scrapper.niche == 'aasi'
        assert scrapper.api_key == valid_api_key
        assert scrapper.daily_quota_limit == 20000
        assert scrapper.quota_used == 0
        assert 'places' in scrapper.ensamble
        assert scrapper.ensamble['status'] == 'in_progress'
        assert scrapper.ensamble['quota_used'] == 0
        assert isinstance(scrapper.ensamble['stats'], dict)

    def test_constructor_loads_search_terms(self, valid_api_key, mock_database_handler):
        """Test that constructor loads search terms from JSON file."""
        scrapper = InformationScrapper(niche='aasi', api_key=valid_api_key)

        assert isinstance(scrapper.search_terms, list)
        assert len(scrapper.search_terms) == 3
        assert 'aparelhos auditivos' in scrapper.search_terms
        assert 'loja aparelhos auditivos' in scrapper.search_terms
        assert 'centros auditivos' in scrapper.search_terms

    def test_constructor_with_invalid_niche(self, valid_api_key, mock_database_handler):
        """Test constructor with niche that has no search terms."""
        scrapper = InformationScrapper(niche='invalid_niche', api_key=valid_api_key)

        assert scrapper.search_terms == []

    def test_constructor_initializes_database_handler(
        self, valid_api_key, mock_database_handler
    ):
        """Test that constructor initializes database handler."""
        scrapper = InformationScrapper(niche='aasi', api_key=valid_api_key)

        assert scrapper.db_handler is not None


# Tests for Helper Methods
class TestInformationScrapperHelperMethods:
    """Tests for InformationScrapper helper methods."""

    def test_calculate_distance_same_location(
        self, valid_api_key, mock_database_handler
    ):
        """Test distance calculation for same location returns 0."""
        scrapper = InformationScrapper(niche='aasi', api_key=valid_api_key)

        distance = scrapper._calculate_distance(
            -23.5505199, -46.6333094, -23.5505199, -46.6333094
        )

        assert distance == 0.0

    def test_calculate_distance_different_locations(
        self, valid_api_key, mock_database_handler
    ):
        """Test distance calculation between different locations."""
        scrapper = InformationScrapper(niche='aasi', api_key=valid_api_key)

        # Distance between two points in São Paulo (approx 2.4 km)
        distance = scrapper._calculate_distance(
            -23.5505199,
            -46.6333094,  # Av Paulista
            -23.5525199,
            -46.6553094,  # ~2.4km away
        )

        assert distance > 2000  # Should be more than 2km
        assert distance < 3000  # Should be less than 3km

    def test_is_duplicate_location_within_threshold(
        self, valid_api_key, mock_database_handler
    ):
        """Test duplicate detection when location is within 50m threshold."""
        scrapper = InformationScrapper(niche='aasi', api_key=valid_api_key)

        existing_places = [
            {
                'name': 'Existing Place',
                'geometry': {'location': {'lat': -23.5505199, 'lng': -46.6333094}},
            }
        ]

        # Check location 30 meters away (within threshold)
        duplicate = scrapper._is_duplicate_location(
            -23.5505199 + 0.0003, -46.6333094, existing_places  # ~33m north
        )

        assert duplicate is not None
        assert duplicate['name'] == 'Existing Place'

    def test_is_duplicate_location_outside_threshold(
        self, valid_api_key, mock_database_handler
    ):
        """Test that locations outside 50m threshold are not duplicates."""
        scrapper = InformationScrapper(niche='aasi', api_key=valid_api_key)

        existing_places = [
            {
                'name': 'Existing Place',
                'geometry': {'location': {'lat': -23.5505199, 'lng': -46.6333094}},
            }
        ]

        # Check location 100 meters away (outside threshold)
        duplicate = scrapper._is_duplicate_location(
            -23.5505199 + 0.001, -46.6333094, existing_places  # ~111m north
        )

        assert duplicate is None

    def test_check_quota_within_limit(self, valid_api_key, mock_database_handler):
        """Test quota check when within limit."""
        scrapper = InformationScrapper(
            niche='aasi', api_key=valid_api_key, daily_quota_limit=1000
        )
        scrapper.quota_used = 500

        result = scrapper._check_quota(100)

        assert result is True
        assert scrapper.ensamble['status'] == 'in_progress'

    def test_check_quota_exceeds_limit(self, valid_api_key, mock_database_handler):
        """Test quota check when exceeding limit."""
        scrapper = InformationScrapper(
            niche='aasi', api_key=valid_api_key, daily_quota_limit=1000
        )
        scrapper.quota_used = 950

        result = scrapper._check_quota(100)

        assert result is False
        assert scrapper.ensamble['status'] == 'partial_quota_exceeded'
        assert 'quota limit reached' in scrapper.ensamble['status_reason'].lower()


# Tests for collect_places Method
class TestInformationScrapperCollectPlaces:
    """Tests for the collect_places method."""

    @patch('src.models.scrappers.information_scrapper.requests.get')
    @patch('src.models.scrappers.information_scrapper.requests.post')
    def test_collect_places_successful_collection(
        self,
        mock_post,
        mock_get,
        valid_api_key,
        mock_database_handler,
        mock_text_search_response,
        mock_place_details_response,
    ):
        """Test successful place collection with text search and details."""
        # Mock text search POST response
        mock_text_response = Mock()
        mock_text_response.json.return_value = mock_text_search_response
        mock_text_response.raise_for_status.return_value = None
        mock_post.return_value = mock_text_response

        # Mock place details GET response
        mock_details_response = Mock()
        mock_details_response.json.return_value = mock_place_details_response
        mock_details_response.raise_for_status.return_value = None
        mock_get.return_value = mock_details_response

        scrapper = InformationScrapper(niche='aasi', api_key=valid_api_key)
        scrapper.collect_places(city='SÃO PAULO', state='SP')

        # Verify places were collected
        assert len(scrapper.ensamble['places']) > 0
        assert scrapper.ensamble['status'] in ['completed', 'partial_quota_exceeded']
        assert scrapper.quota_used > 0

    @patch('src.models.scrappers.information_scrapper.requests.get')
    @patch('src.models.scrappers.information_scrapper.requests.post')
    def test_collect_places_handles_pagination(
        self,
        mock_post,
        mock_get,
        valid_api_key,
        mock_database_handler,
        mock_text_search_response_with_pagination,
        mock_place_details_response,
    ):
        """Test that collect_places handles pagination correctly."""
        # First page with nextPageToken
        mock_first_page = Mock()
        mock_first_page.json.return_value = mock_text_search_response_with_pagination
        mock_first_page.raise_for_status.return_value = None

        # Second page without nextPageToken
        mock_second_page = Mock()
        mock_second_page.json.return_value = {'places': []}
        mock_second_page.raise_for_status.return_value = None

        mock_post.side_effect = [mock_first_page, mock_second_page]
        mock_get.return_value = mock_place_details_response

        scrapper = InformationScrapper(niche='aasi', api_key=valid_api_key)
        scrapper.collect_places(city='SÃO PAULO', state='SP')

        # Verify multiple API calls were made
        assert mock_post.call_count >= 2
        assert scrapper.ensamble['stats']['text_searches'] >= 1

    @patch('src.models.scrappers.information_scrapper.requests.post')
    def test_collect_places_deduplicates_by_place_id(
        self, mock_post, valid_api_key, mock_database_handler
    ):
        """Test that duplicate place_ids are filtered out."""
        # Response with duplicate place_ids (new API format)
        mock_response = Mock()
        mock_response.json.return_value = {
            'places': [
                {
                    'id': 'duplicate-id',
                    'displayName': {'text': 'Place 1'},
                    'location': {'latitude': -23.55, 'longitude': -46.63},
                },
                {
                    'id': 'duplicate-id',  # Same ID
                    'displayName': {'text': 'Place 1 Again'},
                    'location': {'latitude': -23.56, 'longitude': -46.64},
                },
            ],
        }
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        scrapper = InformationScrapper(niche='aasi', api_key=valid_api_key)
        scrapper.collect_places(city='SÃO PAULO', state='SP')

        # Should only collect one place
        assert scrapper.ensamble['stats']['duplicates_by_place_id'] >= 1

    @patch('src.models.scrappers.information_scrapper.requests.post')
    def test_collect_places_stops_on_quota_exceeded(
        self,
        mock_post,
        valid_api_key,
        mock_database_handler,
        mock_text_search_response,
    ):
        """Test that collection stops when quota is exceeded."""
        mock_response = Mock()
        mock_response.json.return_value = mock_text_search_response
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        # Set very low quota limit
        scrapper = InformationScrapper(
            niche='aasi', api_key=valid_api_key, daily_quota_limit=50
        )
        scrapper.collect_places(city='SÃO PAULO', state='SP')

        # Should stop due to quota
        assert scrapper.ensamble['status'] == 'partial_quota_exceeded'
        assert 'quota' in scrapper.ensamble['status_reason'].lower()

    def test_collect_places_with_no_search_terms(
        self, valid_api_key, mock_database_handler
    ):
        """Test collect_places when no search terms are available."""
        scrapper = InformationScrapper(niche='invalid_niche', api_key=valid_api_key)
        scrapper.collect_places(city='SÃO PAULO', state='SP')

        assert scrapper.ensamble['status'] == 'failed_no_search_terms'
        assert len(scrapper.ensamble['places']) == 0

    @patch('src.models.scrappers.information_scrapper.requests.get')
    def test_collect_places_handles_api_errors(
        self, mock_requests, valid_api_key, mock_database_handler
    ):
        """Test that API errors are handled gracefully."""
        mock_requests.side_effect = Exception('API Connection Error')

        scrapper = InformationScrapper(niche='aasi', api_key=valid_api_key)
        scrapper.collect_places(city='SÃO PAULO', state='SP')

        # Should handle error gracefully
        assert scrapper.ensamble['status'] in [
            'failed_api_error',
            'completed_no_results',
        ]


# Tests for collect_details Method
class TestInformationScrapperCollectDetails:
    """Tests for the collect_details method."""

    def test_collect_details_with_valid_places(
        self, valid_api_key, mock_database_handler
    ):
        """Test collect_details with places already collected."""
        scrapper = InformationScrapper(niche='aasi', api_key=valid_api_key)

        # Manually add places to ensamble
        scrapper.ensamble['places'] = [
            {
                'place_id': 'test-id-1',
                'name': 'Test Place 1',
                'city': 'SÃO PAULO',
                'state': 'SP',
                'formatted_address': 'Test Address 1',
                'formatted_phone_number': '(11) 1234-5678',
            },
            {
                'place_id': 'test-id-2',
                'name': 'Test Place 2',
                'city': 'SÃO PAULO',
                'state': 'SP',
                'formatted_address': 'Test Address 2',
                'formatted_phone_number': '(11) 8765-4321',
            },
        ]

        scrapper.collect_details()

        assert hasattr(scrapper, 'places')
        assert len(scrapper.places) == 2
        assert scrapper.places[0]['name'] == 'Test Place 1'
        assert scrapper.places[0]['phone'] == '(11) 1234-5678'
        assert scrapper.places[1]['name'] == 'Test Place 2'

    def test_collect_details_raises_error_without_places(
        self, valid_api_key, mock_database_handler
    ):
        """Test that collect_details raises error when no places collected."""
        scrapper = InformationScrapper(niche='aasi', api_key=valid_api_key)

        # Remove places from ensamble
        del scrapper.ensamble['places']

        with pytest.raises(ValueError, match='No places collected'):
            scrapper.collect_details()

    def test_collect_details_handles_missing_fields(
        self, valid_api_key, mock_database_handler
    ):
        """Test collect_details handles places with missing fields."""
        scrapper = InformationScrapper(niche='aasi', api_key=valid_api_key)

        # Add place with missing fields
        scrapper.ensamble['places'] = [
            {
                'place_id': 'test-id',
                'name': 'Incomplete Place',
                # Missing city, state, address, phone
            }
        ]

        scrapper.collect_details()

        assert len(scrapper.places) == 1
        assert scrapper.places[0]['name'] == 'Incomplete Place'
        assert scrapper.places[0]['address'] == ''
        assert scrapper.places[0]['phone'] == ''


# Integration-style Tests
class TestInformationScrapperIntegration:
    """Integration tests for InformationScrapper with real-like scenarios."""

    @patch('src.models.scrappers.information_scrapper.requests.get')
    @patch('src.models.scrappers.information_scrapper.requests.post')
    def test_full_collection_workflow(
        self,
        mock_post,
        mock_get,
        valid_api_key,
        mock_database_handler,
        mock_text_search_response,
        mock_place_details_response,
    ):
        """Test complete workflow: text search -> details -> database save."""
        # Setup mock responses
        mock_text = Mock()
        mock_text.json.return_value = mock_text_search_response
        mock_text.raise_for_status.return_value = None
        mock_post.return_value = mock_text

        mock_details = Mock()
        mock_details.json.return_value = mock_place_details_response
        mock_details.raise_for_status.return_value = None
        mock_get.return_value = mock_details

        scrapper = InformationScrapper(niche='aasi', api_key=valid_api_key)
        scrapper.collect_places(city='SÃO PAULO', state='SP')

        # Verify complete workflow
        assert len(scrapper.ensamble['places']) > 0
        assert scrapper.quota_used > 0
        assert scrapper.ensamble['stats']['text_searches'] > 0

        # Verify database operations were called
        if (
            mock_database_handler.put_item.called
            or mock_database_handler.update_item.called
        ):
            assert (
                scrapper.ensamble['stats']['new_places']
                + scrapper.ensamble['stats']['updated_places']
                > 0
            )

    @patch('src.models.scrappers.information_scrapper.requests.post')
    def test_statistics_tracking(
        self,
        mock_post,
        valid_api_key,
        mock_database_handler,
        mock_text_search_response,
    ):
        """Test that statistics are properly tracked throughout collection."""
        mock_response = Mock()
        mock_response.json.return_value = mock_text_search_response
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        scrapper = InformationScrapper(niche='aasi', api_key=valid_api_key)
        scrapper.collect_places(city='SÃO PAULO', state='SP')

        stats = scrapper.ensamble['stats']
        assert 'text_searches' in stats
        assert 'details_fetched' in stats
        assert 'duplicates_by_place_id' in stats
        assert 'duplicates_by_location' in stats
        assert 'new_places' in stats
        assert 'updated_places' in stats
        assert 'skipped_places' in stats
