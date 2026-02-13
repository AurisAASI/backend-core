"""
Centralized configuration module for environment-based settings.

This module provides a unified way to access environment-specific configuration
across the entire backend-core project, supporting both dev and prod environments.
"""

import os
from pathlib import Path
from typing import Literal, Optional

from dotenv import load_dotenv

# Load environment variables from .env file
# Locate .env file from the project root (find parent directory containing backend-core structure)
_current_dir = Path(__file__).resolve().parent  # src/shared
_project_root = _current_dir.parent.parent  # Go up to project root
_env_file = _project_root / '.env'

if _env_file.exists():
    load_dotenv(_env_file, override=True)
else:
    # Fallback to current working directory
    load_dotenv(override=True)


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

    @property
    def leads_table_name(self) -> str:
        """Get the leads DynamoDB table name for current stage."""
        return os.environ.get('LEADS_TABLE', f'{self.stage}-auris-core-leads')

    @property
    def communication_history_table_name(self) -> str:
        """Get the communication history DynamoDB table name for current stage."""
        return os.environ.get(
            'COMMUNICATION_HISTORY_TABLE',
            f'{self.stage}-auris-core-communication-history',
        )

    @property
    def import_status_table_name(self) -> str:
        """Get the import status DynamoDB table name for current stage."""
        return os.environ.get(
            'IMPORT_STATUS_TABLE',
            f'{self.stage}-auris-core-import-status',
        )

    @property
    def auth_codes_table_name(self) -> str:
        """Get the authentication codes DynamoDB table name for current stage."""
        return os.environ.get(
            'AUTH_CODES_TABLE',
            f'{self.stage}-auris-auth-codes',
        )

    # TODO Vericicar a chamada dessa função aqui que elenca as tabelas... talvez está em desuso e possa ser removida
    def get_table_name(
        self, table_type: Literal['companies', 'places', 'leads', 'leads']
    ) -> str:
        """
        Get DynamoDB table name by type.

        Args:
            table_type: Type of table ('companies', 'places', or 'leads')

        Returns:
            Full table name with stage prefix

        Raises:
            ValueError: If table_type is invalid
        """
        if table_type == 'companies':
            return self.companies_table_name
        elif table_type == 'places':
            return self.places_table_name
        elif table_type == 'leads':
            return self.leads_table_name
        else:
            raise ValueError(
                f"Invalid table_type: '{table_type}'. Must be 'companies', 'places', or 'leads'"
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

    @property
    def company_federal_scraper_task_queue_url(self) -> str:
        """Get the company federal scraper task queue URL."""
        return os.environ.get('COMPANY_FEDERAL_SCRAPER_TASK_QUEUE_URL', '')

    @property
    def company_federal_scraper_task_queue_name(self) -> str:
        """Get the company federal scraper task queue name for current stage."""
        return f'backend-core-{self.stage}-company-federal-scraper-tasks'

    @property
    def operations_queue_url(self) -> str:
        """Get the operations queue URL for hub lambda routing."""
        return os.environ.get('OPERATIONS_QUEUE_URL', '')

    @property
    def operations_queue_name(self) -> str:
        """Get the operations queue name for current stage."""
        return f'backend-core-{self.stage}-gl-operations-queue'

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

    # AWS Cognito Configuration
    @property
    def cognito_user_pool_id(self) -> str:
        """
        Get the Cognito User Pool ID for current stage.

        Returns:
            User Pool ID for the current environment (dev or prod)
        """
        env_key = f'COGNITO_USER_POOL_ID_{self.stage.upper()}'
        pool_id = os.environ.get(env_key, '')

        if not pool_id:
            raise ValueError(f'Missing required environment variable: {env_key}')

        return pool_id

    @property
    def cognito_user_pool_id_dev(self) -> Optional[str]:
        """
        Get the Cognito User Pool ID for dev stage.

        Returns:
            User Pool ID for dev environment
        """
        return os.environ.get('COGNITO_USER_POOL_ID_DEV', '')

    @property
    def cognito_user_pool_id_prod(self) -> Optional[str]:
        """
        Get the Cognito User Pool ID for prod stage.

        Returns:
            User Pool ID for prod environment
        """
        return os.environ.get('COGNITO_USER_POOL_ID_PROD', '')

    @property
    def cognito_app_client_id_dev(self) -> str:
        """
        Get the Cognito App Client ID for dev stage.

        Returns:
            App Client ID for dev environment

        Raises:
            ValueError: If not configured
        """
        client_id = os.environ.get('COGNITO_APP_CLIENT_ID_DEV', '')
        if not client_id:
            raise ValueError(
                'Missing required environment variable: COGNITO_APP_CLIENT_ID_DEV'
            )
        return client_id

    @property
    def cognito_app_client_id_prod(self) -> str:
        """
        Get the Cognito App Client ID for prod stage.

        Returns:
            App Client ID for prod environment

        Raises:
            ValueError: If not configured
        """
        client_id = os.environ.get('COGNITO_APP_CLIENT_ID_PROD', '')
        if not client_id:
            raise ValueError(
                'Missing required environment variable: COGNITO_APP_CLIENT_ID_PROD'
            )
        return client_id

    @property
    def cognito_app_client_secret_dev(self) -> Optional[str]:
        """
        Get the Cognito App Client Secret for dev stage (optional).

        Returns:
            App Client Secret for dev environment or empty string
        """
        return os.environ.get('COGNITO_APP_CLIENT_SECRET_DEV', '')

    @property
    def cognito_app_client_secret_prod(self) -> Optional[str]:
        """
        Get the Cognito App Client Secret for prod stage (optional).

        Returns:
            App Client Secret for prod environment or empty string
        """
        return os.environ.get('COGNITO_APP_CLIENT_SECRET_PROD', '')

    # AWS SES Configuration
    @property
    def ses_from_email(self) -> str:
        """
        Get the SES from email address for current stage.

        Returns:
            SES email address for the current environment
        """
        env_key = f'SES_FROM_EMAIL_{self.stage.upper()}'
        from_email = os.environ.get(env_key, '')

        if not from_email:
            raise ValueError(f'Missing required environment variable: {env_key}')

        return from_email

    # Authentication Configuration
    @property
    def auth_code_validity_minutes(self) -> int:
        """Get the authentication code validity period in minutes."""
        return int(os.environ.get('AUTH_CODE_VALIDITY_MINUTES', '5'))

    @property
    def auth_code_max_attempts(self) -> int:
        """Get the maximum number of code verification attempts."""
        return int(os.environ.get('AUTH_CODE_MAX_ATTEMPTS', '3'))

    @property
    def auth_code_length(self) -> int:
        """Get the authentication code length."""
        return int(os.environ.get('AUTH_CODE_LENGTH', '6'))

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
