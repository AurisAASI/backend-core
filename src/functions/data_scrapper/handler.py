import json
from http import HTTPStatus
from typing import Any, Dict, Optional

from aws_lambda_powertools import Logger

from src.models.scrappers import aasi_scrapper

# Configure basic logging
logger = Logger(service='aasi-scraper')


def data_scrapper(event, context):
    """Collects information on the web in a determined city/state"""
    logger.info(f'Processing event: {event}')

    try:
        # From payload extracts the following information
        # Location:
        # city_name
        # state_name
        # Other parameters as needed
        # niche (options: aasi, orl, geria, audiologist)
        # Then task is passed to the properly configured scrapper (another lambda
        # function)
        pass

    except Exception as e:
        logger.error(f'Error processing request: {str(e)}')
        error_response = {'error': 'Internal Server Error', 'details': str(e)}

        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps(error_response),
        }
