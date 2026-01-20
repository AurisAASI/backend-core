"""
Lambda handler for fetching lead reminders.

Exposes GET /leads/fetch_reminders (API key protected) and filters leads in DynamoDB by:
- reminderDate null/absent
- reminderDate older than past 7 days
- reminderDate between past 7 days and next 60 days

Results are scoped by required query params: companyID (lowercased).
Returns { "leads": [...] } with 200 even when empty.
"""

from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from typing import Any, Dict, List

from auris_tools.databaseHandlers import DatabaseHandler
from aws_lambda_powertools import Logger
from boto3.dynamodb.conditions import Attr

from src.shared.settings import Settings
from src.shared.utils import response, validate_request_source

logger = Logger(service='fetch_leads_reminders')
settings = Settings()



CORS_HEADERS = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type,x-api-key,X-Amz-Date,Authorization,X-Api-Key',
    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET,PUT,DELETE',
}


def _validate_and_normalize_params(event: Dict[str, Any]) -> Dict[str, str]:
    """Extract and lowercase required query params."""
    params = event.get('queryStringParameters') or {}
    company_id = (params.get('companyID') or '').strip().lower()
    email = (params.get('userEmail') or '').strip().lower()

    missing = [name for name, value in [('companyID', company_id),('userEmail', email)] if not value]
    if missing:
        raise ValueError(f"Missing required query parameters: {', '.join(missing)}")

    return {'companyID': company_id, 'userEmail': email}

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
    filter_condition = (
        Attr('companyID').eq(filters['companyID']) &
        (
            Attr('reminderDate').not_exists() |
            Attr('reminderDate').eq('') |
            Attr('reminderDate').lt(bounds['past']) |
            Attr('reminderDate').between(bounds['past'], bounds['future'])
        )
    )

    result = leads_db.scan(filters=filter_condition)

    return result.get('Items', [])


def _map_leads(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Project items to response schema and stamp updatedAt."""
    now_iso = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
    projected = []
    for item in items:
        projected.append(
            {
                'leadID': item.get('leadID'),
                'fullName': item.get('fullName'),
                'phone': item.get('phone'),
                'statusLead': item.get('statusLead'),
                'reminderDate': item.get('reminderDate'),
                'entryDate': item.get('entryDate'),
                'updatedAt': now_iso,
            }
        )
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
        
        

        filters = _validate_and_normalize_params(event)
        logger.append_keys(companyID=filters['companyID'], email=filters['userEmail'])

        # TODO Por hora, existe um campo userEmail no body, mas depois será trocado pela logica do COgnito
        # TODO Remove the validate_request_source logic after integrating with Cognito
        logger.info('Validating the request source (user email)')
        validate_request_source(filters['userEmail'])
        logger.info(f"Request source validated for user: {filters['userEmail']}")

        items = _scan_leads(filters)
        if not items:
            logger.info('Scan returned an empty result set')
            return response(status_code=HTTPStatus.OK, message={'message': 'No leads found in the company database'}, headers=CORS_HEADERS)
        
        leads = _map_leads(items)

        return response(status_code=HTTPStatus.OK, message={'leads': leads}, headers=CORS_HEADERS)

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
