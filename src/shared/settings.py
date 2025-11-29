"""
Centralized configuration module for environment-based settings.

This module provides a unified way to access environment-specific configuration
across the entire backend-core project, supporting both dev and prod environments.
"""

import os
from typing import Literal, Optional


class Settings:
    """
    Centralized settings manager for environment-specific configuration.

    Automatically detects the environment stage (dev/prod) and provides
    dynamic resource names, API keys, and configuration values.

    Attributes:
        stage: Current deployment stage ('dev' or 'prod')
        region: AWS region
        account_id: AWS account ID
    """

    VALID_STAGES = ['dev', 'prod']

    def __init__(self):
        """Initialize settings from environment variables."""
        self.stage = os.environ.get('STAGE', 'dev').lower()
        self.region = os.environ.get('REGION', 'us-east-1')
        self.account_id = os.environ.get('ACCOUNT_ID', '')

        # Validate stage
        if self.stage not in self.VALID_STAGES:
            raise ValueError(
                f"Invalid STAGE: '{self.stage}'. Must be one of {self.VALID_STAGES}"
            )

    # DynamoDB Table Names
    @property
    def companies_table_name(self) -> str:
        """Get the companies DynamoDB table name for current stage."""
        return os.environ.get('COMPANIES_TABLE', f'{self.stage}-auris-core-companies')

    @property
    def places_table_name(self) -> str:
        """Get the places DynamoDB table name for current stage."""
        return os.environ.get('PLACES_TABLE', f'{self.stage}-auris-core-places')

    def get_table_name(self, table_type: Literal['companies', 'places']) -> str:
        """
        Get DynamoDB table name by type.

        Args:
            table_type: Type of table ('companies' or 'places')

        Returns:
            Full table name with stage prefix

        Raises:
            ValueError: If table_type is invalid
        """
        if table_type == 'companies':
            return self.companies_table_name
        elif table_type == 'places':
            return self.places_table_name
        else:
            raise ValueError(
                f"Invalid table_type: '{table_type}'. Must be 'companies' or 'places'"
            )

    # SQS Queue Configuration
    @property
    def scraper_task_queue_url(self) -> str:
        """Get the scraper task queue URL."""
        return os.environ.get('SCRAPER_TASK_QUEUE_URL', '')

    @property
    def scraper_task_queue_name(self) -> str:
        """Get the scraper task queue name for current stage."""
        return f'backend-core-{self.stage}-scraper-tasks'

    @property
    def website_scraper_task_queue_url(self) -> str:
        """Get the website scraper task queue URL."""
        return os.environ.get('WEBSITE_SCRAPER_TASK_QUEUE_URL', '')

    @property
    def website_scraper_task_queue_name(self) -> str:
        """Get the website scraper task queue name for current stage."""
        return f'backend-core-{self.stage}-website-scraper-tasks'

    # Google Places API Configuration
    @property
    def google_places_api_key(self) -> str:
        """
        Get the Google Places API key for current stage.

        Returns:
            API key for the current environment (dev or prod)
        """
        env_key = f'GOOGLE_PLACES_API_KEY_{self.stage.upper()}'
        api_key = os.environ.get(env_key, '')

        if not api_key:
            raise ValueError(f'Missing required environment variable: {env_key}')

        return api_key

    @property
    def google_places_daily_quota_limit(self) -> int:
        """
        Get the Google Places API daily quota limit for current stage.

        Returns:
            Daily quota limit (default varies by stage: dev=10000, prod=20000)
        """
        # Try stage-specific quota first
        env_key = f'GOOGLE_PLACES_DAILY_QUOTA_LIMIT_{self.stage.upper()}'
        quota_str = os.environ.get(env_key)

        if quota_str:
            return int(quota_str)

        # Default quotas by stage
        default_quotas = {
            'dev': 10000,
            'prod': 20000,
        }

        return default_quotas.get(self.stage, 10000)

    # Google Gemini API Configuration
    @property
    def gemini_api_key(self) -> str:
        """
        Get the Google Gemini API key for current stage.

        Returns:
            API key for the current environment (dev or prod)
        """
        env_key = f'GEMINI_API_KEY_{self.stage.upper()}'
        api_key = os.environ.get(env_key, '')

        if not api_key:
            raise ValueError(f'Missing required environment variable: {env_key}')

        return api_key

    # Helper Methods
    def get_resource_name(
        self, resource_type: str, suffix: Optional[str] = None
    ) -> str:
        """
        Generate a stage-prefixed resource name.

        Args:
            resource_type: Type of resource (e.g., 'lambda', 'queue', 'topic')
            suffix: Optional suffix to append

        Returns:
            Formatted resource name: {stage}-{resource_type}[-{suffix}]
        """
        base_name = f'{self.stage}-{resource_type}'
        if suffix:
            return f'{base_name}-{suffix}'
        return base_name

    def is_production(self) -> bool:
        """Check if running in production environment."""
        return self.stage == 'prod'

    def is_development(self) -> bool:
        """Check if running in development environment."""
        return self.stage == 'dev'

    def __repr__(self) -> str:
        """String representation of settings."""
        return (
            f"Settings(stage='{self.stage}', region='{self.region}', "
            f"companies_table='{self.companies_table_name}', "
            f"places_table='{self.places_table_name}')"
        )


# Global settings instance for easy import
settings = Settings()
