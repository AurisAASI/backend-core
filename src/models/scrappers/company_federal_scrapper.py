"""
Company Federal Scrapper for fetching federal company data from Brasil API.

This module fetches comprehensive company information from the Brasil API public service
using CNPJ numbers extracted from websites. Brasil API is a collaborative, modern solution
that consumes multiple sources including "Minha Receita" project.

No token required for basic queries - fast, reliable, and well-documented.
"""

import time
from datetime import datetime, timezone
from typing import Dict, Optional

import requests
from auris_tools.databaseHandlers import DatabaseHandler
from aws_lambda_powertools import Logger

from src.models.scrappers import BaseScrapper
from src.shared.settings import settings

# Configure logger
logger = Logger(service='company-federal-scraper')

# Brasil API constants
BRASIL_API_URL = 'https://brasilapi.com.br/api/cnpj/v1'
REQUEST_TIMEOUT = 15  # Increased from 5 to 15 seconds (connection + read timeout)
RATE_LIMIT_DELAY = 0.1  # 0.1 second between requests
MAX_RETRIES = 2  # Retry failed requests up to 2 times


class CompanyFederalScrapper(BaseScrapper):
    """
    Scrapper for federal company data from Brasil API.

    This class fetches official company information from Brazil's federal registry
    using CNPJ numbers and saves enriched data to DynamoDB.

    Brasil API aggregates data from multiple sources (Minha Receita project, etc)
    and provides fast, reliable access without requiring API tokens.

    Args:
        company_id (str): UUID of the company in the database
        cnpj (str): Clean CNPJ number (14 digits only)
    """

    def __init__(self, company_id: str, cnpj: str):
        """Initialize the CompanyFederalScrapper."""
        super().__init__()

        # Set up default boto3 session with explicit region
        import boto3

        boto3.setup_default_session(region_name=settings.region)
        logger.info(f'boto3 default session configured for region: {settings.region}')

        self.company_id = company_id
        self.cnpj = cnpj
        self.ensamble = {
            'status': 'in_progress',
            'status_reason': '',
            'data_fetched': False,
        }

        # Initialize database handler
        try:
            self.db_handler = DatabaseHandler(table_name=settings.companies_table_name)
            logger.info(
                f'Database handler initialized for table: {settings.companies_table_name}'
            )
        except Exception as e:
            logger.warning(f'DatabaseHandler initialization failed: {str(e)}')
            self.db_handler = None

        logger.info(
            f'CompanyFederalScrapper initialized - '
            f'Company: {company_id}, CNPJ: {cnpj}, Stage: {settings.stage}'
        )

    def _fetch_opencnpj_data(self) -> Optional[Dict]:
        """
        Fetch company data from Brasil API with retry logic.

        Brasil API consolidates data from multiple sources (Minha Receita project, etc)
        and provides fast, reliable access without requiring API tokens.

        Returns:
            Dict with company data or None on error
        """
        url = f'{BRASIL_API_URL}/{self.cnpj}'
        logger.info(f'Fetching data from Brasil API: {url}')

        for attempt in range(1, MAX_RETRIES + 2):  # +2 for initial attempt + retries
            try:
                # Rate limiting
                if attempt > 1:
                    wait_time = 2 ** (
                        attempt - 1
                    )  # Exponential backoff: 2, 4, 8 seconds
                    logger.info(
                        f'Retry attempt {attempt} for CNPJ {self.cnpj} - '
                        f'waiting {wait_time}s before retry'
                    )
                    time.sleep(wait_time)
                else:
                    time.sleep(RATE_LIMIT_DELAY)

                response = requests.get(
                    url,
                    timeout=REQUEST_TIMEOUT,
                    headers={'User-Agent': 'AurisBot/1.0 (+https://leadcontrol.ia.br)'},
                )

                # Check status code
                if response.status_code == 200:
                    data = response.json()
                    logger.info(
                        f'Successfully fetched Brasil API data for CNPJ: {self.cnpj} '
                        f'(attempt {attempt})'
                    )
                    return data
                elif response.status_code == 404:
                    logger.warning(f'CNPJ not found in Brasil API: {self.cnpj}')
                    self.ensamble['status'] = 'completed'
                    self.ensamble[
                        'status_reason'
                    ] = 'CNPJ not found in federal registry'
                    return None
                elif response.status_code == 429:
                    # Rate limit - retry might help
                    if attempt <= MAX_RETRIES:
                        logger.warning(
                            f'Rate limit exceeded for Brasil API (attempt {attempt}), '
                            f'will retry'
                        )
                        continue
                    else:
                        logger.error(
                            f'Rate limit exceeded for Brasil API (all retries failed)'
                        )
                        self.ensamble['status'] = 'failed'
                        self.ensamble['status_reason'] = 'API rate limit exceeded'
                        return None
                else:
                    logger.error(
                        f'Brasil API returned status {response.status_code}: '
                        f'{response.text}'
                    )
                    self.ensamble['status'] = 'failed'
                    self.ensamble[
                        'status_reason'
                    ] = f'API error: HTTP {response.status_code}'
                    return None

            except requests.exceptions.ConnectTimeout as e:
                logger.warning(
                    f'Connection timeout fetching from Brasil API (attempt {attempt}/{MAX_RETRIES + 1}): {str(e)}'
                )
                if attempt > MAX_RETRIES:
                    logger.error(
                        f'All retry attempts exhausted for CNPJ {self.cnpj}. '
                        f'Brasil API connection timeout.'
                    )
                    self.ensamble['status'] = 'failed'
                    self.ensamble[
                        'status_reason'
                    ] = f'API connection timeout (after {attempt} attempts)'
                    return None
                continue

            except requests.exceptions.ReadTimeout as e:
                logger.warning(
                    f'Read timeout fetching from Brasil API (attempt {attempt}/{MAX_RETRIES + 1}): {str(e)}'
                )
                if attempt > MAX_RETRIES:
                    logger.error(
                        f'All retry attempts exhausted for CNPJ {self.cnpj}. Brasil API read timeout.'
                    )
                    self.ensamble['status'] = 'failed'
                    self.ensamble[
                        'status_reason'
                    ] = f'API read timeout (after {attempt} attempts)'
                    return None
                continue

            except requests.exceptions.Timeout as e:
                logger.warning(
                    f'General timeout fetching from Brasil API (attempt {attempt}/{MAX_RETRIES + 1}): {str(e)}'
                )
                if attempt > MAX_RETRIES:
                    logger.error(
                        f'All retry attempts exhausted for CNPJ {self.cnpj}. Brasil API timeout.'
                    )
                    self.ensamble['status'] = 'failed'
                    self.ensamble[
                        'status_reason'
                    ] = f'API request timeout (after {attempt} attempts)'
                    return None
                continue

            except requests.exceptions.RequestException as e:
                logger.error(
                    f'Request error fetching Brasil API data (attempt {attempt}): {str(e)}'
                )
                self.ensamble['status'] = 'failed'
                self.ensamble['status_reason'] = f'API request failed: {str(e)}'
                return None

            except Exception as e:
                logger.error(
                    f'Unexpected error fetching Brasil API data (attempt {attempt}): {str(e)}'
                )
                self.ensamble['status'] = 'failed'
                self.ensamble['status_reason'] = f'Unexpected error: {str(e)}'
                return None

        # All retries exhausted
        logger.error(
            f'Failed to fetch Brasil API data for CNPJ {self.cnpj} after all retry attempts'
        )
        self.ensamble['status'] = 'failed'
        self.ensamble['status_reason'] = 'API request failed - max retries exceeded'
        return None

    def _save_to_database(self, federal_data: Dict) -> bool:
        """
        Save fetched federal data to DynamoDB companies table.

        Args:
            federal_data: Federal company data from Brasil API

        Returns:
            True if successful, False otherwise
        """
        if not self.db_handler:
            logger.warning('Database handler not available, skipping database save')
            return False

        try:
            logger.info(f'Saving federal data for company {self.company_id}')

            # Prepare update data
            update_data = {
                'federal_data': federal_data,
                'federal_scraping_status': self.ensamble['status'],
                'federal_scraping_reason': self.ensamble['status_reason'],
                'federal_scraped_at': datetime.now(timezone.utc).isoformat(),
            }

            # Update company record
            self.db_handler.update_item(
                key={'companyID': self.company_id},
                updates=update_data,
                primary_key='companyID',
            )

            logger.info(
                f'Successfully saved federal data for company {self.company_id}'
            )
            return True

        except Exception as e:
            logger.error(f'Error saving to database: {str(e)}')
            self.ensamble['status'] = 'failed'
            self.ensamble['status_reason'] = f'Database save failed: {str(e)}'
            return False

    def collect_data(self) -> None:
        """
        Main method to orchestrate federal data collection.

        This method:
        1. Validates the CNPJ number
        2. Fetches data from Brasil API
        3. Saves enriched data to database
        """
        logger.info(
            f'Starting federal data collection for company {self.company_id}, '
            f'CNPJ: {self.cnpj}'
        )

        try:
            # Validate CNPJ
            if not self.cnpj or len(self.cnpj) != 14:
                raise ValueError(f'Invalid CNPJ format: {self.cnpj}')

            # Fetch data from Brasil API
            federal_data = self._fetch_opencnpj_data()

            # Check if data was fetched
            if federal_data:
                self.ensamble['data_fetched'] = True
                self.ensamble['status'] = 'completed'
                self.ensamble['status_reason'] = 'Successfully fetched federal data'

                # Save to database
                save_success = self._save_to_database(federal_data)

                if not save_success:
                    # Status already updated in _save_to_database
                    pass
            else:
                # Status already set in _fetch_opencnpj_data
                # Save status to database even if no data
                self._save_to_database({})

            logger.info(
                f'Federal data collection completed - '
                f'Status: {self.ensamble["status"]}, '
                f'Reason: {self.ensamble["status_reason"]}'
            )

        except ValueError as e:
            logger.error(f'Validation error: {str(e)}')
            self.ensamble['status'] = 'failed'
            self.ensamble['status_reason'] = f'Invalid CNPJ: {str(e)}'
            self._save_to_database({})
        except Exception as e:
            logger.error(
                f'Unexpected error during federal data collection: {str(e)}',
                exc_info=True,
            )
            self.ensamble['status'] = 'failed'
            self.ensamble['status_reason'] = f'Collection error: {str(e)}'
            self._save_to_database({})
