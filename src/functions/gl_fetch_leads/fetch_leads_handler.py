"""
Lambda handler for fetching lead reminders.

Exposes GET /leads/fetch_reminders (Cognito protected) and filters leads in DynamoDB by:
- reminderDate null/absent
- reminderDate older than past 7 days
- reminderDate between past 7 days and next 60 days

Results are scoped by required query params: companyID (lowercased).
Returns { "leads": [...] } with 200 even when empty.
"""

import json
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from pathlib import Path
from typing import Any, Dict, List

from auris_tools.databaseHandlers import DatabaseHandler
from aws_lambda_powertools import Logger
from boto3.dynamodb.conditions import Attr

from src.shared.settings import Settings
from src.shared.utils import response

logger = Logger(service='fetch_leads_reminders')
settings = Settings()


# Load schema templates
LEAD_SCHEMA_PATH = (
    Path(__file__).parent.parent.parent
    / 'shared'
    / 'schema'
    / 'gl_new_lead_schema.json'
)
with open(LEAD_SCHEMA_PATH, 'r') as f:
    LEAD_SCHEMA = json.load(f)

CORS_HEADERS = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type,x-api-key,X-Amz-Date,Authorization,X-Api-Key',
    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET,PUT,DELETE',
}


def _validate_and_normalize_params(event: Dict[str, Any]) -> Dict[str, str]:
    """Extract and lowercase required query params."""
    params = event.get('queryStringParameters') or {}
    company_id = (params.get('companyID') or '').strip().lower()

    missing = [name for name, value in [('companyID', company_id)] if not value]
    if missing:
        raise ValueError(f"Missing required query parameters: {', '.join(missing)}")

    return {'companyID': company_id}


def _build_date_bounds() -> Dict[str, str]:
    """Compute UTC ISO date bounds for filtering."""
    now = datetime.now(timezone.utc)
    past_7 = now - timedelta(days=7)
    future_60 = now + timedelta(days=60)

    # Use ISO 8601 with Z to match stored strings
    def iso(dt: datetime) -> str:
        return dt.isoformat().replace('+00:00', 'Z')

    return {'past': iso(past_7), 'future': iso(future_60)}


def _scan_leads(filters: Dict[str, str]) -> List[Dict[str, Any]]:
    """Scan leads table with server-side filter for reminders."""
    bounds = _build_date_bounds()
    leads_db = DatabaseHandler(table_name=settings.leads_table_name)

    # Build boto3 condition expression
    filter_condition = Attr('companyID').eq(filters['companyID']) & (
        Attr('reminderDate').not_exists()
        | Attr('reminderDate').eq('')
        | Attr('reminderDate').lt(bounds['past'])
        | Attr('reminderDate').between(bounds['past'], bounds['future'])
    )

    result = leads_db.scan(filters=filter_condition)

    return result.get('Items', [])


def _map_leads(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Project items to response schema and stamp updatedAt."""
    # Lead schema template that matches gl_new_lead_schema.json
    lead_schema_template = LEAD_SCHEMA.copy()

    now_iso = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
    projected = []

    for item in items:
        # Copy schema template and fill with item data
        lead = lead_schema_template.copy()
        for key in lead:
            if key in item:
                lead[key] = item[key]
        # Always stamp updatedAt with current timestamp
        lead['updatedAt'] = now_iso
        projected.append(lead)

    return projected


def fetch_leads_reminders(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Lambda entrypoint for GET /leads/fetch_reminders."""
    try:
        logger.info('Processing fetch reminders request')

        # Handle preflight
        if event.get('httpMethod') == 'OPTIONS':
            return response(status_code=HTTPStatus.OK, message='', headers=CORS_HEADERS)

        if event.get('httpMethod') != 'GET':
            return response(
                status_code=HTTPStatus.METHOD_NOT_ALLOWED,
                message={'message': 'Method not allowed'},
                headers=CORS_HEADERS,
            )

        # Extract authenticated user email from Cognito authorizer
        try:
            authorizer = event.get('requestContext', {}).get('authorizer', {})
            claims = authorizer.get('claims', {})
            authenticated_email = claims.get('email', '').strip().lower()

            if not authenticated_email:
                logger.error('Missing email in Cognito token')
                return response(
                    status_code=HTTPStatus.UNAUTHORIZED,
                    message={
                        'message': 'Authentication error: email claim missing in token'
                    },
                    headers=CORS_HEADERS,
                )

            logger.append_keys(email=authenticated_email)
            logger.info(f'Request authenticated for user: {authenticated_email}')
        except Exception as exc:
            logger.error(f'Failed to extract authenticated user: {exc}')
            return response(
                status_code=HTTPStatus.UNAUTHORIZED,
                message={
                    'message': 'Authentication error: unable to verify user identity'
                },
                headers=CORS_HEADERS,
            )

        filters = _validate_and_normalize_params(event)
        logger.append_keys(companyID=filters['companyID'])

        items = _scan_leads(filters)
        if not items:
            logger.info('Scan returned an empty result set')
            return response(
                status_code=HTTPStatus.OK,
                message={'message': 'No leads found in the company database'},
                headers=CORS_HEADERS,
            )
        logger.info(f'Scan returned {len(items)} items')

        leads = _map_leads(items)

        logger.info(f'Mapped leads with total of {len(leads)} entries')
        return response(
            status_code=HTTPStatus.OK, message={'leads': leads}, headers=CORS_HEADERS
        )

    except ValueError as exc:
        logger.warning(f'Validation error: {exc}')
        return response(
            status_code=HTTPStatus.BAD_REQUEST,
            message={'message': str(exc)},
            headers=CORS_HEADERS,
        )
    except Exception as exc:  # pragma: no cover - safety net
        logger.error(f'Unexpected error fetching reminders: {exc}', exc_info=True)
        return response(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            message={'message': 'Internal server error'},
            headers=CORS_HEADERS,
        )
