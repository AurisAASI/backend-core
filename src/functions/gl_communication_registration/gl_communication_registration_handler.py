"""
Lambda handler for registering communications in the Auris CRM system.

This module provides functionality for communication registration with:
- Payload validation (leadID, companyID, communication data)
- Lead and company existence validation
- Communication history creation
- Lead record update with new communication ID
"""

import json
import uuid
from datetime import datetime, timezone
from http import HTTPStatus
from pathlib import Path
from typing import Any, Dict, Optional

from auris_tools.databaseHandlers import DatabaseHandler
from auris_tools.utils import generate_uuid
from aws_lambda_powertools import Logger

from src.shared.settings import Settings
from src.shared.utils import response

logger = Logger(service='gl-communication-registration')
settings = Settings()

CORS_HEADERS = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type,x-api-key,X-Amz-Date,Authorization,X-Api-Key',
    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET,PUT,DELETE',
}

# Load communication history schema template
COMM_HISTORY_SCHEMA_PATH = (
    Path(__file__).parent.parent.parent
    / 'shared'
    / 'schema'
    / 'communication_history_schema.json'
)
with open(COMM_HISTORY_SCHEMA_PATH, 'r') as f:
    COMM_HISTORY_SCHEMA = json.load(f)

# Load lead schema template for validation
LEAD_SCHEMA_PATH = (
    Path(__file__).parent.parent.parent
    / 'shared'
    / 'schema'
    / 'gl_new_lead_schema.json'
)
with open(LEAD_SCHEMA_PATH, 'r') as f:
    LEAD_SCHEMA = json.load(f)


def _extract_payload_from_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract payload from event based on trigger type (API Gateway or SQS).

    Args:
        event: Lambda invocation event

    Returns:
        Extracted payload dictionary

    Raises:
        ValueError: If payload cannot be extracted or parsed
    """
    # Check if it's an API Gateway event
    if 'body' in event and isinstance(event.get('body'), str):
        logger.info('Detected API Gateway trigger')
        try:
            body = json.loads(event['body'])
            return body.get('update_data', body)
        except json.JSONDecodeError as e:
            logger.error(f'Failed to parse API Gateway body JSON: {str(e)}')
            raise ValueError('Invalid JSON in request body')

    # Check if it's an SQS event
    if 'update_data' in event:
        logger.info('Detected SQS/Queue Manager trigger')
        return event.get('update_data', {})

    # Check if it's direct payload (for flexibility)
    if 'leadID' in event or 'observations' in event:
        logger.info('Detected direct payload trigger')
        return event

    raise ValueError('Unable to determine trigger type or extract payload')


def communication_registration(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for registering a new communication for an existing lead.

    This function can be invoked in two ways:
    1. Via API Gateway POST request with Cognito authentication (authenticated user email from Cognito token)
    2. Via SQS event from gl_queue_manager (enriched payload with update_data)

    Process:
    1. Detect trigger type and extract payload accordingly
    2. For API Gateway: extract authenticated user email from Cognito authorizer context
    3. Validate required fields (leadID, companyID, message/observations)
    4. Validate lead exists and belongs to company
    5. Create communication history entry
    6. Update lead record with new communication ID
    7. Optionally update lead status

    Args:
        event: Lambda invocation event. Can be:
            - API Gateway event with 'body' containing JSON payload and Cognito claims
            - SQS event with enriched payload in 'update_data'
        context: Lambda context object

    Returns:
        Success response (200) with communication ID
        Error response (400) on validation errors
        Error response (401) on authentication errors
        Error response (500) on unexpected errors

    Response Body (200 OK):
        {
            "message": "Communication registered successfully",
            "communicationID": "comm-uuid",
            "leadID": "lead-uuid"
        }
    """
    try:
        logger.info('Processing communication registration request')
        logger.info(f'Event: {json.dumps(event)}')

        # Handle preflight
        if event.get('httpMethod') == 'OPTIONS':
            return response(status_code=HTTPStatus.OK, message='', headers=CORS_HEADERS)

        # Extract authenticated user from Cognito if API Gateway trigger
        authenticated_email = None
        is_api_gateway = 'body' in event and isinstance(event.get('body'), str)

        if is_api_gateway:
            try:
                authorizer = event.get('requestContext', {}).get('authorizer', {})
                claims = authorizer.get('claims', {})
                authenticated_email = claims.get('email', '').strip().lower()

                if not authenticated_email:
                    logger.error('Missing email in Cognito token')
                    return response(
                        status_code=401,
                        message={
                            'message': 'Authentication error: email claim missing in token'
                        },
                        headers=CORS_HEADERS,
                    )

                logger.info(f'Request authenticated for user: {authenticated_email}')
            except Exception as e:
                logger.error(f'Failed to extract authenticated user: {str(e)}')
                return response(
                    status_code=401,
                    message={
                        'message': 'Authentication error: unable to verify user identity'
                    },
                    headers=CORS_HEADERS,
                )

        # Extract payload based on trigger type
        form_data = _extract_payload_from_event(event)

        if not form_data:
            raise ValueError('Payload is required')

        logger.info(f'Registering update lead data from form input: {form_data}')
        # Initialize database handlers
        leads_db = DatabaseHandler(table_name=settings.leads_table_name)
        communication_db = DatabaseHandler(
            table_name=settings.communication_history_table_name
        )

        # Update lead with new communication ID and optionally status
        try:
            new_comm_id, lead_id = _update_lead_and_communication(
                lead_update_data=form_data,
                leads_db=leads_db,
                communication_db=communication_db,
            )
        except ValueError as e:
            if str(e).startswith('Observation content is duplicated'):
                return response(
                    status_code=HTTPStatus.BAD_REQUEST,
                    message={'error': str(e)},
                    headers=CORS_HEADERS,
                )
            raise

        logger.info(
            f'Communication registered successfully: {new_comm_id} for lead {lead_id}'
        )

        return response(
            status_code=HTTPStatus.OK,
            message={
                'message': 'Communication registered successfully',
                'communicationID': new_comm_id,
                'leadID': lead_id,
            },
            headers=CORS_HEADERS,
        )

    except ValueError as e:
        logger.warning(f'Validation error: {str(e)}')
        return response(
            status_code=HTTPStatus.BAD_REQUEST,
            message={'error': str(e)},
            headers=CORS_HEADERS,
        )

    except Exception as e:
        logger.error(
            f'Unexpected error registering communication: {str(e)}', exc_info=True
        )
        return response(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            message={'error': 'Internal server error'},
            headers=CORS_HEADERS,
        )


def _validate_lead_payload(payload: Dict[str, Any], metadata: Dict[str, Any]) -> None:
    """
    Validate required fields in payload and metadata.

    Args:
        payload: Communication data payload
        metadata: Metadata with userEmail, companyID, etc.

    Raises:
        ValueError: If required fields are missing or invalid
    """
    # Validate metadata fields
    required_metadata = ['companyID']
    missing_metadata = [field for field in required_metadata if not metadata.get(field)]
    if missing_metadata:
        raise ValueError(
            f"Missing required metadata fields: {', '.join(missing_metadata)}"
        )

    # Validate payload fields
    required_fields = ['leadID', 'message']
    missing_fields = [field for field in required_fields if not payload.get(field)]
    if missing_fields:
        raise ValueError(
            f"Missing required payload fields: {', '.join(missing_fields)}"
        )

    # Validate message is not empty
    message = payload.get('message', '').strip()
    if not message:
        raise ValueError('Communication message cannot be empty')


def create_communication_entry(
    lead_id: str,
    company_id: str,
    assigned_user: Optional[str],
    communication_date: Optional[str],
    status: Optional[str],
    source: Optional[str],
    message: str,
    communication_db: DatabaseHandler,
) -> str:
    """
    Create a new communication history entry.

    Args:
        company_id: Company ID
        lead_id: Lead ID
        message: Communication message
        status: Lead status (optional)
        assigned_user: User who handled the communication (optional)
        source: Communication source (optional)
        communication_db: DatabaseHandler for communication history table

    Returns:
        Communication ID of created entry
    """
    # Generate communication ID and timestamp
    comm_id = 'comm-' + generate_uuid()

    # Build communication data using schema template
    comm_data = COMM_HISTORY_SCHEMA.copy()
    comm_data['communicationID'] = comm_id
    comm_data['leadID'] = lead_id
    comm_data['companyID'] = company_id
    comm_data['assignedUser'] = assigned_user
    comm_data['communicationDate'] = communication_date
    comm_data['status'] = status
    comm_data['source'] = source
    comm_data['message'] = message.strip()

    # Remove empty strings
    comm_data = {k: v for k, v in comm_data.items() if v != ''}

    # Insert into DynamoDB
    try:
        communication_db.insert_item(item=comm_data, primary_key='communicationID')
    except Exception as e:
        logger.error(
            f'Error inserting communication entry into DB: {str(e)}', exc_info=True
        )
        raise RuntimeError('Failed to create communication entry in database')

    logger.info(f'Communication history entry created: {comm_id}')
    return comm_id


def _update_lead_and_communication(
    lead_update_data: Dict[str, Any],
    leads_db: DatabaseHandler,
    communication_db: DatabaseHandler,
) -> None:
    """
    Update lead record with new communication ID and optionally update status.

    Args:
        lead_update_data: Data to update lead record
        leads_db: DatabaseHandler for leads table
        communication_db: DatabaseHandler for communication history table
    """
    lead_id = lead_update_data.get('leadID')
    lead_db_info = leads_db._deserialize_item(
        leads_db.get_item(key={'leadID': lead_id})
    )
    updated_at = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')

    # Collect new infos that come from form data
    new_lead_info = _collect_new_lead_info(lead_update_data, lead_db_info)
    if __isObservationContentDuplicated(
        lead_update_data.get('observations'),
        lead_db_info.get('communicationHistoryIds'),
        communication_db,
    ):
        raise ValueError(
            'Observation content is duplicated from existing communications'
        )

    # Get existing communication history IDs and creating new communication entry
    comm_history_ids = lead_db_info.get('communicationHistoryIds', [])
    new_comm_id = create_communication_entry(
        lead_id=lead_update_data.get('leadID'),
        company_id=lead_db_info.get('companyID'),
        assigned_user=new_lead_info.get('assigned_user')
        if new_lead_info.get('assigned_user')
        else lead_db_info.get('assignedUser'),
        communication_date=updated_at,
        status=new_lead_info.get('statusLead')
        if new_lead_info.get('statusLead')
        else lead_db_info.get('statusLead'),
        source=new_lead_info.get('source')
        if new_lead_info.get('source')
        else lead_db_info.get('source'),
        message=lead_update_data.get('observations'),
        communication_db=communication_db,
    )

    # Add new communication ID
    comm_history_ids.append(new_comm_id)

    # Update the lead database record
    input_updates = {
        **new_lead_info,
        'communicationHistoryIds': comm_history_ids,
        'updatedAt': updated_at,
    }
    leads_db.update_item(
        key={'leadID': lead_id},
        primary_key='leadID',
        updates=input_updates,
    )
    logger.info(
        f'Updated lead record with new communication and lead info: {input_updates}'
    )

    logger.info(
        f'Lead {lead_id} updated with communication {new_comm_id} '
        f'({len(comm_history_ids)} total communications)'
    )

    return new_comm_id, lead_id


def _collect_new_lead_info(
    lead_form_data: Dict[str, Any], lead_db_data: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Compare lead form data with database data and extract validated differences.

    This function performs the following steps:
    1. Compares lead_form_data with lead_db_data to identify differences
    2. Validates that all difference keys exist in the lead schema
    3. Returns only the validated difference fields that can be updated

    Args:
        lead_form_data: Updated lead information from form submission
        lead_db_data: Current lead record from database

    Returns:
        Dictionary containing only the fields that differ and are valid per schema

    Raises:
        ValueError: If any difference field is not found in the lead schema

    Example:
        >>> form_data = {'fullName': 'John Doe', 'phone': '555-1234'}
        >>> db_data = {'fullName': 'Jane Doe', 'phone': '555-5678', 'email': 'jane@example.com'}
        >>> result = collect_new_lead_info(form_data, db_data)
        >>> # result = {'fullName': 'John Doe', 'phone': '555-1234'}
    """
    if not isinstance(lead_form_data, dict) or not isinstance(lead_db_data, dict):
        raise ValueError('Both lead_form_data and lead_db_data must be dictionaries')

    # Get valid schema field names
    valid_schema_fields = set(LEAD_SCHEMA.keys())
    logger.info(f'Valid schema fields available: {len(valid_schema_fields)} fields')

    # Extract differences between form data and database data
    differences = {}
    for field, form_value in lead_form_data.items():
        db_value = lead_db_data.get(field)
        # Include field if it differs (handles None/empty cases)
        if form_value != db_value:
            differences[field] = form_value
            logger.info(
                f'Field difference detected: {field} (old: {db_value}, new: {form_value})'
            )

    logger.info(f'Total differences found: {len(differences)}')

    # Validate all difference fields exist in schema
    invalid_fields = [
        field
        for field in differences.keys()
        if field not in valid_schema_fields and field != 'observations'
    ]
    if invalid_fields:
        error_msg = f'Invalid fields in differences (not in schema): {invalid_fields}'
        logger.error(error_msg)
        raise ValueError(error_msg)

    logger.info(f'All {len(differences)} difference fields validated against schema')
    return differences


def __isObservationContentDuplicated(
    observation_content: Optional[str],
    communication_history_ids: Optional[list],
    communication_db,
) -> bool:
    """
    Check if the observation content is duplicated from existing communications.

    Args:
        observation_content: New observation content to check
        communication_history_ids: List of communication IDs to check against
        communication_db: Database interface to retrieve communication records
    Returns:
        True if duplicated, False otherwise
    """
    if not observation_content:
        # Empty content cannot be duplicate and will not be saved in the database
        return True

    # Here we would normally check against existing communications in the database.
    existing_communications = []
    if communication_history_ids:
        for comm_id in communication_history_ids:
            comm_record = communication_db.get_item(key={'communicationID': comm_id})
            if communication_db.item_is_serialized(comm_record):
                comm_record = communication_db._deserialize_item(comm_record)
            if comm_record and 'message' in comm_record:
                existing_communications.append(comm_record['message'])

    for existing in existing_communications:
        if observation_content.strip() == existing.strip():
            logger.info('Duplicate observation content found.')
            return True

    return False
