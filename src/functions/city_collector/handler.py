import json
import os
from http import HTTPStatus
from typing import Any, Dict, Optional

import boto3
import requests
from aws_lambda_powertools import Logger

from src.shared.settings import settings
from src.shared.utils import response

# Configure basic logging
logger = Logger(service='city-collector')


def city_collector(event, context):
    logger.info(f'Processing event: {event}')

    try:
        # Extract payload data from event, handling both API Gateway and EventBridge
        payload = extract_payload(event)

        # Validate the payload
        validation_error = validate_payload(payload)
        if validation_error:
            return response(
                message=validation_error, status_code=HTTPStatus.BAD_REQUEST
            )

        city = payload['city'].upper()
        state = payload['state'].upper()
        niche = payload.get('niche', '').upper()

        # Step 1: Check if the city/state is valid for Brasil region
        state_data = requests.get(
            f'https://brasilapi.com.br/api/ibge/uf/v1/{state}'
        ).json()
        if state_data.get('response_code') == HTTPStatus.NOT_FOUND:
            return response(
                message={'error': f'State name not found: {state}'},
                status_code=HTTPStatus.NOT_FOUND,
            )

        city_data = requests.get(
            f'https://brasilapi.com.br/api/ibge/municipios/v1/{state}?providers=dados-abertos-br,gov,wikipedia'
        ).json()
        if city not in [city['nome'].upper() for city in city_data]:
            return response(
                message={
                    'error': f'City name not found in the state - City: {city} - State: {state}'
                },
                status_code=HTTPStatus.NOT_FOUND,
            )

        logger.info(
            f'Processing collection - Stage: {settings.stage}, '
            f'City: {city}, State: {state}, Niche: {niche}'
        )

        # Step 2: Queue scraping task for the city/state
        region = os.environ.get('AWS_REGION_NAME', settings.region)
        sqs_client = boto3.client('sqs', region_name=region)
        sqs_response = sqs_client.send_message(
            QueueUrl=os.environ.get('SCRAPER_TASK_QUEUE_URL'),
            MessageBody=json.dumps(
                {
                    'city': city,
                    'state': state,
                    'niche': niche,
                }
            ),
        )

        logger.info(
            f"Queued scraping task with message ID: {sqs_response['MessageId']}"
        )

        # Create response body
        response_body = {
            'message': 'City collection initiated',
            'city': city,
            'state': state,
            'niche': niche,
        }

        # Check if event is from API Gateway (needs formatted response) or EventBridge
        if 'httpMethod' in event or 'requestContext' in event:
            # Return formatted API Gateway response
            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',  # Enable CORS
                },
                'body': json.dumps(response_body),
            }
        else:
            # Return simple response for EventBridge events
            return {'success': True, 'data': response_body}

    except Exception as e:
        logger.error(f'Error processing request: {str(e)}')
        error_response = {'error': 'Internal Server Error', 'details': str(e)}

        # Check if event is from API Gateway or EventBridge
        if 'httpMethod' in event or 'requestContext' in event:
            return {
                'statusCode': 500,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps(error_response),
            }
        else:
            return {'success': False, 'error': error_response}


def extract_payload(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract payload from different event sources (API Gateway or EventBridge)

    Args:
        event (Dict[str, Any]): Lambda event

    Returns:
        Dict[str, Any]: Extracted payload with normalized structure
    """
    # Check if this is an API Gateway event
    if 'body' in event:
        if isinstance(event['body'], str):
            payload = json.loads(event.get('body', '{}'))
        else:
            payload = event.get('body', {})
    # Check if this is an EventBridge event
    elif 'detail' in event:
        payload = event.get('detail', {})
    # Fallback for direct invocation or other event sources
    else:
        payload = event

    return payload


def validate_payload(payload: Dict[str, Any]) -> Optional[Dict[str, str]]:
    """
    Validate that payload contains required fields

    Args:
        payload (Dict[str, Any]): Extracted payload

    Returns:
        Optional[Dict[str, str]]: Error dict if validation fails, None if valid
    """
    if not payload.get('city'):
        return {'error': 'Missing required field: city'}
    if not payload.get('state'):
        return {'error': 'Missing required field: state'}
    if not payload.get('niche'):
        return {'error': 'Missing required field: niche'}

    # Validate that city is in state (simple check)
    city = payload.get('city', '').lower()
    state = payload.get('state', '').lower()

    # Additional validation could be added here if needed

    return None
