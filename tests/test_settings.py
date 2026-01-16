"""
Test cases for the Settings class.

This test file validates the centralized settings functionality including:
- Stage detection and validation
- Dynamic table name generation
- Google API key resolution
- Quota limit configuration
- Resource naming utilities
"""

import os
import sys
from unittest.mock import patch

import pytest

# Add src directory to the path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))


class TestSettings:
    """Tests for Settings class initialization and configuration."""

    @patch.dict(os.environ, {'STAGE': 'dev', 'REGION': 'us-east-1'})
    def test_settings_dev_environment(self):
        """Test settings initialization in dev environment."""
        from src.shared.settings import Settings

        settings = Settings()

        assert settings.stage == 'dev'
        assert settings.region == 'us-east-1'
        assert settings.is_development() is True
        assert settings.is_production() is False

    @patch.dict(os.environ, {'STAGE': 'prod', 'REGION': 'us-east-1'})
    def test_settings_prod_environment(self):
        """Test settings initialization in prod environment."""
        from src.shared.settings import Settings

        settings = Settings()

        assert settings.stage == 'prod'
        assert settings.region == 'us-east-1'
        assert settings.is_development() is False
        assert settings.is_production() is True

    @patch.dict(os.environ, {'STAGE': 'invalid'})
    def test_settings_invalid_stage_raises_error(self):
        """Test that invalid stage raises ValueError."""
        from src.shared.settings import Settings

        with pytest.raises(ValueError, match='Invalid STAGE'):
            Settings()

    @patch.dict(os.environ, {'STAGE': 'dev'})
    def test_companies_table_name_dev(self):
        """Test companies table name generation for dev."""
        from src.shared.settings import Settings

        settings = Settings()

        assert settings.companies_table_name == 'dev-auris-core-companies'

    @patch.dict(os.environ, {'STAGE': 'prod'})
    def test_companies_table_name_prod(self):
        """Test companies table name generation for prod."""
        from src.shared.settings import Settings

        settings = Settings()

        assert settings.companies_table_name == 'prod-auris-core-companies'

    @patch.dict(os.environ, {'STAGE': 'dev'})
    def test_places_table_name_dev(self):
        """Test places table name generation for dev."""
        from src.shared.settings import Settings

        settings = Settings()

        assert settings.places_table_name == 'dev-auris-core-places'

    @patch.dict(os.environ, {'STAGE': 'prod'})
    def test_places_table_name_prod(self):
        """Test places table name generation for prod."""
        from src.shared.settings import Settings

        settings = Settings()

        assert settings.places_table_name == 'prod-auris-core-places'

    @patch.dict(os.environ, {'STAGE': 'dev', 'COMPANIES_TABLE': 'custom-companies'})
    def test_companies_table_name_from_env(self):
        """Test companies table name override from environment variable."""
        from src.shared.settings import Settings

        settings = Settings()

        assert settings.companies_table_name == 'custom-companies'

    @patch.dict(os.environ, {'STAGE': 'dev'})
    def test_get_table_name_companies(self):
        """Test get_table_name for companies."""
        from src.shared.settings import Settings

        settings = Settings()

        assert settings.get_table_name('companies') == 'dev-auris-core-companies'

    @patch.dict(os.environ, {'STAGE': 'dev'})
    def test_get_table_name_places(self):
        """Test get_table_name for places."""
        from src.shared.settings import Settings

        settings = Settings()

        assert settings.get_table_name('places') == 'dev-auris-core-places'

    @patch.dict(os.environ, {'STAGE': 'dev'})
    def test_get_table_name_invalid_type(self):
        """Test get_table_name with invalid type raises ValueError."""
        from src.shared.settings import Settings

        settings = Settings()

        with pytest.raises(ValueError, match='Invalid table_type'):
            settings.get_table_name('invalid')

    @patch.dict(
        os.environ, {'STAGE': 'dev', 'GOOGLE_PLACES_API_KEY_DEV': 'test-dev-key'}
    )
    def test_google_api_key_dev(self):
        """Test Google API key retrieval for dev environment."""
        from src.shared.settings import Settings

        settings = Settings()

        assert settings.google_places_api_key == 'test-dev-key'

    @patch.dict(
        os.environ, {'STAGE': 'prod', 'GOOGLE_PLACES_API_KEY_PROD': 'test-prod-key'}
    )
    def test_google_api_key_prod(self):
        """Test Google API key retrieval for prod environment."""
        from src.shared.settings import Settings

        settings = Settings()

        assert settings.google_places_api_key == 'test-prod-key'

    @patch.dict(os.environ, {'STAGE': 'dev'}, clear=True)
    def test_google_api_key_missing_raises_error(self):
        """Test that missing API key raises ValueError."""
        from src.shared.settings import Settings

        settings = Settings()

        with pytest.raises(ValueError, match='Missing required environment variable'):
            _ = settings.google_places_api_key

    @patch.dict(
        os.environ, {'STAGE': 'dev', 'GOOGLE_PLACES_DAILY_QUOTA_LIMIT_DEV': '5000'}
    )
    def test_quota_limit_from_env_dev(self):
        """Test quota limit from environment variable for dev."""
        from src.shared.settings import Settings

        settings = Settings()

        assert settings.google_places_daily_quota_limit == 5000

    @patch.dict(os.environ, {'STAGE': 'dev'}, clear=True)
    def test_quota_limit_default_dev(self):
        """Test default quota limit for dev environment."""
        from src.shared.settings import Settings

        settings = Settings()

        assert settings.google_places_daily_quota_limit == 10000

    @patch.dict(os.environ, {'STAGE': 'prod'}, clear=True)
    def test_quota_limit_default_prod(self):
        """Test default quota limit for prod environment."""
        from src.shared.settings import Settings

        settings = Settings()

        assert settings.google_places_daily_quota_limit == 20000

    @patch.dict(os.environ, {'STAGE': 'dev'})
    def test_scraper_task_queue_name(self):
        """Test scraper task queue name generation."""
        from src.shared.settings import Settings

        settings = Settings()

        assert settings.scraper_task_queue_name == 'backend-core-dev-scraper-tasks'

    @patch.dict(
        os.environ, {'STAGE': 'dev', 'SCRAPER_TASK_QUEUE_URL': 'https://queue-url'}
    )
    def test_scraper_task_queue_url(self):
        """Test scraper task queue URL from environment."""
        from src.shared.settings import Settings

        settings = Settings()

        assert settings.scraper_task_queue_url == 'https://queue-url'

    @patch.dict(os.environ, {'STAGE': 'dev'})
    def test_get_resource_name_without_suffix(self):
        """Test resource name generation without suffix."""
        from src.shared.settings import Settings

        settings = Settings()

        assert settings.get_resource_name('lambda') == 'dev-lambda'

    @patch.dict(os.environ, {'STAGE': 'prod'})
    def test_get_resource_name_with_suffix(self):
        """Test resource name generation with suffix."""
        from src.shared.settings import Settings

        settings = Settings()

        assert settings.get_resource_name('queue', 'tasks') == 'prod-queue-tasks'

    @patch.dict(os.environ, {'STAGE': 'dev', 'REGION': 'us-west-2'})
    def test_settings_repr(self):
        """Test string representation of Settings."""
        from src.shared.settings import Settings

        settings = Settings()

        repr_str = repr(settings)
        assert "stage='dev'" in repr_str
        assert "region='us-west-2'" in repr_str
        assert "companies_table='dev-auris-core-companies'" in repr_str
        assert "places_table='dev-auris-core-places'" in repr_str
