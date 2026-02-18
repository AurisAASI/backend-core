"""
Lambda handler for creating new leads in the Auris CRM system.

This module provides HTTP endpoint functionality for lead creation with:
- API Gateway event payload extraction and validation
- Phone number normalization and duplicate checking (per company)
- Company existence validation
- Complete lead record creation with defaults and timestamps
- GSI-based duplicate detection using companyID-phone-index
"""

import json
import re
import uuid
from datetime import datetime, timezone
from http import HTTPStatus
from pathlib import Path
from typing import Any, Dict, Optional

from auris_tools.databaseHandlers import DatabaseHandler
from aws_lambda_powertools import Logger

from src.shared.settings import Settings
from src.shared.utils import (
    check_duplicate_phone,
    normalize_phone,
    response,
    validate_company_exists,
    validate_request_source,
)

logger = Logger(service='add_new_lead')
settings = Settings()

# CORS headers to include in all responses
CORS_HEADERS = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type,x-api-key,X-Amz-Date,Authorization,X-Api-Key',
    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET,PUT,DELETE',
}

# Message constraints
MAX_OBSERVATION_LENGTH = 5000  # Maximum characters for observation field

# Load schema templates
LEAD_SCHEMA_PATH = (
    Path(__file__).parent.parent.parent
    / 'shared'
    / 'schema'
    / 'gl_new_lead_schema.json'
)
with open(LEAD_SCHEMA_PATH, 'r') as f:
    LEAD_SCHEMA = json.load(f)

COMM_HISTORY_SCHEMA_PATH = (
    Path(__file__).parent.parent.parent
    / 'shared'
    / 'schema'
    / 'communication_history_schema.json'
)
with open(COMM_HISTORY_SCHEMA_PATH, 'r') as f:
    COMM_HISTORY_SCHEMA = json.load(f)


def extract_payload(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract and parse payload from API Gateway event.

    Args:
        event: API Gateway event containing body

    Returns:
        Parsed payload dictionary

    Raises:
        ValueError: If body is missing or invalid JSON
    """
    body = event.get('body')
    if not body:
        raise ValueError('Request body is required')

    try:
        if isinstance(body, str):
            return json.loads(body)
        return body
    except json.JSONDecodeError as e:
        raise ValueError(f'Invalid JSON in request body: {str(e)}')


def validate_payload(payload: Dict[str, Any]) -> None:
    """
    Validate required fields and enum values in payload.

    Args:
        payload: Request payload dictionary

    Raises:
        ValueError: If required fields are missing or values are invalid
    """
    # Required fields
    required_fields = ['fullName', 'phone', 'city', 'companyID', 'source']
    missing_fields = [field for field in required_fields if not payload.get(field)]

    if missing_fields:
        raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")


def create_initial_communication(
    company_id: str,
    lead_id: str,
    assigned_user: Optional[str],
    source: str,
    status: str,
    message: str,
    communication_db: DatabaseHandler,
) -> str:
    """
    Create initial communication history entry for a new lead.

    Args:
        company_id: Company ID
        assigned_user: Assigned user ID (optional)
        source: Lead source
        status: Lead status
        message: Communication message
        communication_db: DatabaseHandler for communication history table

    Returns:
        Communication ID of created entry
    """
    # Generate communication ID and timestamp
    comm_id = 'comm-' + str(uuid.uuid4())
    current_timestamp = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')

    # Build communication data using schema template
    comm_data = COMM_HISTORY_SCHEMA.copy()
    comm_data['communicationID'] = comm_id
    comm_data['companyID'] = company_id
    comm_data['leadID'] = lead_id
    comm_data['assignedUser'] = assigned_user if assigned_user else ''
    comm_data['communicationDate'] = current_timestamp
    comm_data['status'] = status
    comm_data['source'] = source
    comm_data['message'] = message

    # Remove empty strings
    comm_data = {k: v for k, v in comm_data.items() if v != ''}

    # Insert into DynamoDB
    communication_db.insert_item(item=comm_data, primary_key='communicationID')

    logger.info(f'Communication history entry created: {comm_id}')
    return comm_id


def add_new_lead(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for creating a new lead via HTTP POST request.

    Authentication: AWS Cognito User Pool (user must be authenticated)

    Process:
    1. Extract authenticated user email from Cognito authorizer context
    2. Extract and validate payload from API Gateway event
    3. Normalize phone number (strip non-digits, validate length)
    4. Validate required fields and source enum value
    5. Validate company exists in companies table
    6. Check for duplicate phone within same company using GSI
    7. Create initial communication history entry
    8. Generate leadId and build complete lead record with defaults
    9. Insert lead into DynamoDB
    10. Return success response with leadId

    Args:
        event: API Gateway HTTP event with:
            - body: JSON containing lead data
            - requestContext.authorizer.claims: Cognito user claims (email, sub, etc.)
        context: Lambda context object

    Returns:
        API Gateway response with 201 status and leadId on success
        API Gateway response with 400 status on validation errors
        API Gateway response with 401 status on authentication errors
        API Gateway response with 500 status on unexpected errors

    Expected Request Body:
        {
            "fullName": "João Silva",
            "phone": "+55 11 98765-4321",  # Will be normalized to digits only
            "email": "joao@example.com",  # Optional
            "city": "São Paulo",
            "companyID": "COMP001",
            "source": "Auditik Site",
            "allowsMarketing": true,  # Optional, defaults to true
            "communicationObservation": "Initial contact via website"  # Optional
        }

    Response Body (201 Created):
        {
            "message": "Lead created successfully",
            "leadId": "uuid-generated-lead-id"
        }
    """
    try:
        logger.info('Processing new lead creation request')

        # Handle OPTIONS preflight request
        if event.get('httpMethod') == 'OPTIONS':
            logger.info('Handling OPTIONS preflight request')
            return response(status_code=200, message='', headers=CORS_HEADERS)

        # Extract authenticated user email from Cognito authorizer
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

        # Extract and validate payload
        payload = extract_payload(event)
        validate_payload(payload)

        # Normalize and validate phone
        raw_phone = payload.get('phone', '')
        normalized_phone = normalize_phone(raw_phone)
        logger.info(f"Phone normalized from '{raw_phone}' to '{normalized_phone}'")

        # Initialize database handlers
        companies_db = DatabaseHandler(table_name=settings.companies_table_name)
        leads_db = DatabaseHandler(table_name=settings.leads_table_name)
        communication_db = DatabaseHandler(
            table_name=settings.communication_history_table_name
        )

        # Validate company exists
        company_id = payload.get('companyID')
        validate_company_exists(company_id, companies_db)
        logger.info(f"Company '{company_id}' validated successfully")

        # Check for duplicate phone within company
        phone_validation = check_duplicate_phone(company_id, normalized_phone, leads_db)
        if phone_validation['status'] != HTTPStatus.OK:
            logger.warning(phone_validation['message'])
            return response(
                status_code=phone_validation['status'],
                message={'message': phone_validation['message']},
                headers=CORS_HEADERS,
            )
        logger.info(f"No duplicate phone found for company '{company_id}'")

        # Generate leadId and timestamps
        lead_id = 'lead-' + str(uuid.uuid4())

        # Create initial communication history entry
        assigned_user = payload.get('assignedUser')
        source = payload.get('source')
        status = (
            payload.get('statusLead')
            if payload.get('statusLead')
            else 'Aguardando contato'
        )

        # Use observation if provided from import, otherwise use default message
        if payload.get('observation'):
            observation = str(payload.get('observation')).strip()
            # Truncate observation to maximum allowed length with warning
            if len(observation) > MAX_OBSERVATION_LENGTH:
                logger.warning(
                    f'Observation truncated from {len(observation)} to '
                    f'{MAX_OBSERVATION_LENGTH} characters for lead {lead_id}'
                )
                observation = observation[:MAX_OBSERVATION_LENGTH]
            initial_message = observation
        else:
            initial_message = f'Lead criado via {source}. Status inicial: {status}.'

        comm_id = create_initial_communication(
            company_id=company_id,
            lead_id=lead_id,
            assigned_user=assigned_user,
            source=source,
            status=status,
            message=initial_message,
            communication_db=communication_db,
        )
        current_timestamp = (
            datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        )

        # Build complete lead data using schema template
        lead_data = LEAD_SCHEMA.copy()

        # Populate required fields
        lead_data['leadID'] = lead_id
        lead_data['companyID'] = company_id
        lead_data['fullName'] = payload.get('fullName')
        lead_data['phone'] = normalized_phone
        lead_data['city'] = payload.get('city')
        lead_data['allowsMarketing'] = payload.get('allowsMarketing', True)
        lead_data['entryDate'] = (
            payload.get('entryDate') if payload.get('entryDate') else current_timestamp
        )
        lead_data['statusLead'] = status
        lead_data['source'] = payload.get('source')
        lead_data['createdAt'] = current_timestamp
        lead_data['updatedAt'] = current_timestamp
        lead_data['reminderDate'] = (
            payload.get('reminderDate')
            if payload.get('reminderDate')
            else current_timestamp
        )

        # Populate optional fields if provided
        if payload.get('email'):
            lead_data['email'] = payload.get('email')
        if payload.get('audiologist'):
            lead_data['audiologist'] = payload.get('audiologist')
        if payload.get('assignedUser'):
            lead_data['assignedUser'] = payload.get('assignedUser')
        if payload.get('statusClassification'):
            lead_data['statusClassification'] = payload.get('statusClassification')
        if payload.get('importID'):
            lead_data['importID'] = payload.get('importID')

        # Set initial communication history with the created communication ID
        lead_data['communicationHistoryIds'] = [comm_id]

        # Remove empty string values to avoid storing unnecessary data in DynamoDB
        lead_data = {k: v for k, v in lead_data.items() if v != ''}

        # Insert lead into DynamoDB
        leads_db.insert_item(item=lead_data, primary_key='leadID')

        logger.info(f'Lead created successfully with ID: {lead_id}')

        return response(
            status_code=201,
            message={'message': 'Lead created successfully', 'leadID': lead_id},
            headers=CORS_HEADERS,
        )

    except ValueError as e:
        logger.warning(f'Validation error: {str(e)}')
        return response(
            status_code=400, message={'message': str(e)}, headers=CORS_HEADERS
        )

    except Exception as e:
        logger.error(f'Unexpected error creating lead: {str(e)}', exc_info=True)
        return response(
            status_code=500,
            message={'message': 'Internal server error'},
            headers=CORS_HEADERS,
        )
