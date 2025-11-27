import json
import os
from http import HTTPStatus
from typing import Any, Dict, Optional

from aws_lambda_powertools import Logger

from src.models.scrappers.information_scrapper import InformationScrapper
from src.shared.settings import settings

# Configure basic logging
logger = Logger(service='data-scraper')


def data_scrapper(event, context):
    """
    Collects information on the web in a determined city/state.

    This function is triggered by a single SQS message from the city_collector.
    It extracts city, state, and niche from the message and initiates
    the appropriate scrapper to collect place data from Google Places API.

    Args:
        event: SQS event containing single message with city/state/niche
        context: Lambda context object

    Returns:
        Dict with success status and collected data summary
    """
    logger.info('Processing SQS event')

    try:
        # Extract the single SQS record
        records = event.get('Records', [])
        if not records:
            logger.error('No SQS records found in event')
            raise ValueError('No SQS records in event')

        record = records[0]
        message_id = record.get('messageId')

        # Extract message body
        body = record.get('body', '{}')
        if isinstance(body, str):
            payload = json.loads(body)
        else:
            payload = body

        logger.info(f'Processing message {message_id}: {payload}')

        # Extract required fields
        city = payload.get('city', '').strip()
        state = payload.get('state', '').strip()
        niche = payload.get('niche', 'aasi').strip().lower()

        # Validate required fields
        if not city or not state:
            error_msg = f'Missing required fields - City: {city}, State: {state}'
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Get API key and quota from settings
        try:
            api_key = settings.google_places_api_key
            quota_limit = settings.google_places_daily_quota_limit
        except ValueError as e:
            logger.error(f'Configuration error: {str(e)}')
            raise

        logger.info(
            f'Starting scraper - Stage: {settings.stage}, City={city}, State={state}, '
            f'Niche={niche}, QuotaLimit={quota_limit}, '
            f'Tables: {settings.companies_table_name}, {settings.places_table_name}'
        )

        # Load valid niches from niche_terms.json
        valid_niches = _load_niches()

        # Instantiate appropriate scrapper based on niche
        if niche in valid_niches:
            scrapper = InformationScrapper(
                niche=niche, api_key=api_key, daily_quota_limit=quota_limit
            )
        else:
            error_msg = f'Unknown niche: {niche}. Valid niches: {valid_niches}'
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Execute place collection
        logger.info(f'Collecting places for {city}, {state}...')
        scrapper.collect_places(city=city, state=state)

        # Check final status
        final_status = scrapper.ensamble.get('status')
        status_reason = scrapper.ensamble.get('status_reason')
        places_collected = len(scrapper.ensamble.get('places', []))
        quota_used = scrapper.ensamble.get('quota_used', 0)

        logger.info(
            f'Scraping completed - Status: {final_status}, '
            f'Reason: {status_reason}, '
            f'Places: {places_collected}, '
            f'Quota used: {quota_used}'
        )

        # Check if scraping failed
        if final_status and final_status.startswith('failed'):
            error_msg = f'Scraping failed for {city}, {state}: {status_reason}'
            logger.error(error_msg)
            raise Exception(error_msg)

        # Log success metrics
        stats = scrapper.ensamble.get('stats', {})
        logger.info(
            f'Scraping stats - '
            f'Text searches: {stats.get("text_searches", 0)}, '
            f'Details fetched: {stats.get("details_fetched", 0)}, '
            f'New places: {stats.get("new_places", 0)}, '
            f'Updated: {stats.get("updated_places", 0)}, '
            f'Skipped: {stats.get("skipped_places", 0)}, '
            f'Duplicates (ID): {stats.get("duplicates_by_place_id", 0)}, '
            f'Duplicates (location): {stats.get("duplicates_by_location", 0)}'
        )

        # Return success response
        return {
            'statusCode': 200,
            'body': json.dumps(
                {
                    'success': True,
                    'message': 'Data scraping completed successfully',
                    'city': city,
                    'state': state,
                    'niche': niche,
                    'status': final_status,
                    'status_reason': status_reason,
                    'places_collected': places_collected,
                    'quota_used': quota_used,
                    'stats': stats,
                }
            ),
        }

    except Exception as e:
        logger.error(
            f'Error in data_scrapper handler: {str(e)}',
            exc_info=True,
        )
        return {
            'statusCode': 500,
            'body': json.dumps(
                {'success': False, 'error': 'Internal Server Error', 'details': str(e)}
            ),
        }


def _load_niches() -> Dict[str, Any]:
    """
    Load valid niches from niche_terms.json file.

    Returns:
        Dict of valid niches
    """
    niche_terms_file = os.path.join(
        os.path.dirname(__file__), '../../models/scrappers/niche_terms.json'
    )
    try:
        with open(niche_terms_file, 'r', encoding='utf-8') as f:
            niche_terms_data = json.load(f)
        valid_niches = list(niche_terms_data.keys())
    except Exception as e:
        logger.error(f'Failed to load niche_terms.json: {str(e)}')
        valid_niches = ['aasi']  # Fallback to default

    return valid_niches
