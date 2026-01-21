"""
Lambda handler for fetching organized lead communication history.

Exposes POST /leads/fetch_history (API key protected) and retrieves
communication records from DynamoDB by a provided list of communicationIDs.

Process:
- Body must include: companyID (lowercased), communicationIDs (array of strings)
- For each communicationID, GetItem from <stage>-auris-core-communication-history
- Validate communication's companyID matches requested companyID
- For each unique leadId found, GetItem from <stage>-auris-core-leads to enrich
  leadName, status, and source
- Output sorted by updatedAt descending

Returns 200 with partial results when possible, even when empty.
"""

import json
from datetime import datetime, timezone
from http import HTTPStatus
from typing import Any, Dict, List, Optional, Tuple

from auris_tools.databaseHandlers import DatabaseHandler
from aws_lambda_powertools import Logger

from src.shared.settings import Settings
from src.shared.utils import response, validate_request_source

logger = Logger(service='fetch_lead_history')
settings = Settings()


CORS_HEADERS = {
	'Access-Control-Allow-Origin': '*',
	'Access-Control-Allow-Headers': 'Content-Type,x-api-key,X-Amz-Date,Authorization,X-Api-Key',
	'Access-Control-Allow-Methods': 'OPTIONS,POST,GET,PUT,DELETE',
}


def _parse_body(event: Dict[str, Any]) -> Dict[str, Any]:
	"""Parse and validate POST body for required fields.

	Expected body JSON:
	{
		"companyID": "...",
		"communicationIDs": ["id1", "id2", ...],
		"userEmail": "optional@example.com"  # temporary until Cognito
	}
	"""
	raw_body = event.get('body') or ''
	try:
		body = json.loads(raw_body) if isinstance(raw_body, str) else (raw_body or {})
	except Exception:
		raise ValueError('Invalid JSON body')

	company_id = (body.get('companyID') or '').strip().lower()
	comm_ids = body.get('communicationIDs')
	user_email = (body.get('userEmail') or '').strip().lower()

	if not company_id:
		raise ValueError('Missing required field: companyID')
	if not isinstance(comm_ids, list) or not all(isinstance(x, str) and x.strip() for x in comm_ids):
		raise ValueError('communicationIDs must be a non-empty array of strings')

	# Normalize IDs by stripping whitespace
	comm_ids = [x.strip() for x in comm_ids]

	return {'companyID': company_id, 'communicationIDs': comm_ids, 'userEmail': user_email}

def _get_communication_by_id(communication_id: str, db: DatabaseHandler) -> Optional[Dict[str, Any]]:
	"""Fetch a single communication record by ID from DynamoDB."""
	try:
		item = db.get_item(key={'communicationID': communication_id})

		if db.item_is_serialized(item):
			item = db._deserialize_item(item)
		
		return item or None
	except Exception as exc:
		logger.warning(f'GetItem failed for communicationID={communication_id}: {exc}')
		return None

def _get_lead_by_id(lead_id: str, db: DatabaseHandler) -> Optional[Dict[str, Any]]:
	"""Fetch a single lead record by ID from DynamoDB."""
	try:
		item = db.get_item(key={'leadID': lead_id})
		if db.item_is_serialized(item):
			item = db._deserialize_item(item)

		return item or None
	except Exception as exc:
		logger.warning(f'GetItem failed for leadID={lead_id}: {exc}')
		return None


def _iso_to_dt(value: Optional[str]) -> Optional[datetime]:
	if not value or not isinstance(value, str):
		return None
	try:
		# Support ISO with trailing Z
		return datetime.fromisoformat(value.replace('Z', '+00:00'))
	except Exception:
		return None


def _extract_lead_fields(lead: Optional[Dict[str, Any]]) -> Tuple[str, str, str]:
	"""Extract leadName, status, source from a lead record, with safe fallbacks."""
	if not lead:
		return ('', '', '')
	lead_name = lead.get('fullName')
	status = lead.get('statusLead')
	source = lead.get('source')
	return (lead_name, status, source)


def _map_history_entry(comm: Dict[str, Any], lead_details: Tuple[str, str, str]) -> Dict[str, Any]:
	lead_name, status, source = lead_details
	# Prefer updatedAt; fallback to communicationDate
	communication_date =  comm.get('communicationDate') if comm.get('communicationDate') else comm.get('updatedAt')
	message = comm.get('message')
	lead_id = comm.get('leadID')
	return {
		'leadId': lead_id,
		'leadName': lead_name,
		'updatedAt': communication_date,
		'status': status,
		'source': source,
		'message': message,
	}


def fetch_lead_history(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
	"""Lambda entrypoint for POST /leads/fetch_history."""
	try:
		logger.info('Processing fetch lead history request')

		# Handle preflight
		if event.get('httpMethod') == 'OPTIONS':
			return response(status_code=HTTPStatus.OK, message='', headers=CORS_HEADERS)

		if event.get('httpMethod') != 'POST':
			return response(
				status_code=HTTPStatus.METHOD_NOT_ALLOWED,
				message={'message': 'Method not allowed'},
				headers=CORS_HEADERS,
			)

		params = _parse_body(event)
		company_id = params['companyID']
		comm_ids = params['communicationIDs']
		user_email = params.get('userEmail') or ''

		logger.append_keys(companyID=company_id)
		if user_email:
			logger.append_keys(email=user_email)
			logger.info('Validating the request source (user email)')
			validate_request_source(user_email)
			logger.info(f'Request source validated for user: {user_email}')

		# Fetch communications and validate companyID
		found_count = 0
		missing_count = 0
		mismatch_count = 0
		communications: List[Dict[str, Any]] = []

		# Initialize DB handlers
		communication_db_handler = DatabaseHandler(table_name=settings.communication_history_table_name)
		leads_db_handler = DatabaseHandler(table_name=settings.leads_table_name)

		for cid in comm_ids:
			item = _get_communication_by_id(cid, communication_db_handler)
			if not item:
				missing_count += 1
				logger.warning(f'Communication not found for communicationID={cid}')
				continue
			# Company validation on the record
			item_company = item.get('companyID')
			if item_company != company_id:
				mismatch_count += 1
				logger.warning(
					f'Company mismatch for communicationID={cid}: item companyID={item_company}, requested={company_id}'
				)
				continue
			communications.append(item)
			found_count += 1

		logger.info(
			f'Communications fetched: found={found_count}, missing={missing_count}, company_mismatch={mismatch_count}'
		)

		if not communications:
			logger.info('No valid communications found, returning empty history')
			return response(
				status_code=HTTPStatus.OK,
				message={'history': []},
				headers=CORS_HEADERS,
			)

		# Fetch lead details for enrichment
		lead_ids = {
			c.get('leadID'): None for c in communications
		}
		# Remove empty keys
		lead_ids = {k: v for k, v in lead_ids.items() if k}

		leads_map: Dict[str, Tuple[str, str, str]] = {}
		for lid in lead_ids.keys():
			lead_item = _get_lead_by_id(lid, leads_db_handler)
			leads_map[lid] = _extract_lead_fields(lead_item)

		# Map output entries
		entries: List[Dict[str, Any]] = []
		for comm in communications:
			lid = comm.get('leadID')
			lead_details = leads_map.get(lid, ('', '', ''))
			entries.append(_map_history_entry(comm, lead_details))

		# Sort by updatedAt desc, using ISO parsing when possible
		entries.sort(
			key=lambda e: (
				_iso_to_dt(e.get('updatedAt')) or datetime.fromtimestamp(0, tz=timezone.utc)
			),
			reverse=True,
		)

		logger.info(f'Returning {len(entries)} history entries after enrichment and sorting')
		return response(
			status_code=HTTPStatus.OK,
			message={'history': entries},
			headers=CORS_HEADERS,
		)

	except ValueError as exc:
		logger.warning(f'Validation error: {exc}')
		return response(
			status_code=HTTPStatus.BAD_REQUEST,
			message={'message': str(exc)},
			headers=CORS_HEADERS,
		)
	except Exception as exc:  # pragma: no cover - safety net
		logger.error(f'Unexpected error fetching lead history: {exc}', exc_info=True)
		return response(
			status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
			message={'message': 'Internal server error'},
			headers=CORS_HEADERS,
		)

