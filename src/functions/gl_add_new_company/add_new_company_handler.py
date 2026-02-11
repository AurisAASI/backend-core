"""
Lambda handler for creating new companies in the Auris CRM system.

This module provides HTTP endpoint functionality for company creation with:
- API Gateway event payload extraction and validation
- User list validation with permission and status enums
- Company name duplicate detection using Levenshtein similarity (90% threshold)
- Filtered scan by city+state+niche for efficient duplicate checking
- Complete company record creation with schema loading
"""

import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from auris_tools.databaseHandlers import DatabaseHandler
from aws_lambda_powertools import Logger

from src.shared.settings import Settings
from src.shared.utils import calculate_similarity_ratio, response

logger = Logger(service='add_new_company')
settings = Settings()

# Valid permission values for users
VALID_PERMISSIONS = ['user', 'admin', 'manager']

# Valid status values for users
VALID_STATUSES = ['ativo', 'inativo', 'pendente']

# Levenshtein similarity threshold for duplicate detection (90%)
SIMILARITY_THRESHOLD = 90.0


def load_company_schema() -> Dict[str, Any]:
    """
    Load company schema from JSON file.

    Returns:
        Dictionary containing the company schema template

    Raises:
        FileNotFoundError: If schema file doesn't exist
        ValueError: If schema file contains invalid JSON
    """
    try:
        # Build path relative to this file
        schema_path = Path(__file__).parent / '../../shared/schema/company_schema.json'

        if not schema_path.exists():
            raise FileNotFoundError(f'Company schema file not found at: {schema_path}')

        with open(schema_path, 'r', encoding='utf-8') as f:
            schema = json.load(f)

        logger.info('Company schema loaded successfully')
        return schema

    except json.JSONDecodeError as e:
        raise ValueError(f'Invalid JSON in company schema file: {str(e)}')
    except Exception as e:
        logger.error(f'Error loading company schema: {str(e)}', exc_info=True)
        raise


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
    Validate required fields in payload.

    Args:
        payload: Request payload dictionary

    Raises:
        ValueError: If required fields are missing
    """
    # Required fields
    required_fields = ['name', 'city', 'state', 'niche']
    missing_fields = [field for field in required_fields if not payload.get(field)]

    if missing_fields:
        raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")


def validate_users(users_input: Optional[Any]) -> List[Dict[str, str]]:
    """
    Validate and format users list.

    Args:
        users_input: Users data from payload (can be None, list of dicts, or invalid)

    Returns:
        Validated list of user dictionaries

    Raises:
        ValueError: If users format is invalid or contains invalid enum values
    """
    # If no users provided, return default user
    if not users_input:
        return [
            {
                'userID': 'user-' + str(uuid.uuid4()),
                'user_name': 'padrao',
                'job': 'administrativo',
                'status': 'ativo',
                'permission': 'user',
            }
        ]

    # Validate it's a list
    if not isinstance(users_input, list):
        raise ValueError('Users must be a list of user dictionaries')

    # If empty list, return default user
    if len(users_input) == 0:
        return [
            {
                'userID': 'user-' + str(uuid.uuid4()),
                'user_name': 'padrao',
                'job': 'administrativo',
                'status': 'ativo',
                'permission': 'user',
            }
        ]

    # Validate each user in the list
    validated_users = []
    required_user_keys = ['user_name', 'job', 'status', 'permission']

    for idx, user in enumerate(users_input):
        # Check it's a dictionary
        if not isinstance(user, dict):
            raise ValueError(f'User at index {idx} must be a dictionary')

        # Check required keys
        missing_keys = [key for key in required_user_keys if key not in user]
        if missing_keys:
            raise ValueError(
                f"User at index {idx} missing required keys: {', '.join(missing_keys)}"
            )

        # Validate permission enum
        permission = user.get('permission')
        if permission not in VALID_PERMISSIONS:
            raise ValueError(
                f"User at index {idx} has invalid permission '{permission}'. "
                f"Must be one of: {', '.join(VALID_PERMISSIONS)}"
            )

        # Validate status enum
        status = user.get('status')
        if status not in VALID_STATUSES:
            raise ValueError(
                f"User at index {idx} has invalid status '{status}'. "
                f"Must be one of: {', '.join(VALID_STATUSES)}"
            )

        # Add validated user
        validated_users.append(
            {
                'userID': 'user-' + str(uuid.uuid4()),
                'user_name': user.get('user_name'),
                'job': user.get('job'),
                'status': status,
                'permission': permission,
            }
        )

    return validated_users


def check_duplicate_company(
    name: str, city: str, state: str, niche: str, db_handler: DatabaseHandler
) -> None:
    """
    Check for duplicate company using Levenshtein similarity on company name.

    Scans companies filtered by city+state+niche to limit results (<100 entries),
    then compares names using 90% similarity threshold.

    Args:
        name: Company name to check
        city: City for filtering scan
        state: State for filtering scan
        niche: Niche for filtering scan
        db_handler: DatabaseHandler instance for companies table

    Raises:
        ValueError: If a similar company name is found (≥90% similarity)
    """
    try:
        # Scan companies table with filters to reduce result set
        result = db_handler.scan(
            table_name=settings.companies_table_name,
            filter_expression='city = :city AND #state = :state AND niche = :niche',
            expression_attribute_names={
                '#state': 'state'  # 'state' is a reserved word in DynamoDB
            },
            expression_attribute_values={
                ':city': city,
                ':state': state,
                ':niche': niche,
            },
        )

        if not result:
            logger.info(
                f"No existing companies found in {city}/{state} with niche '{niche}'"
            )
            return

        logger.info(f'Found {len(result)} existing companies to check for duplicates')

        # Check each company for name similarity
        for company in result:
            existing_name = company.get('name', '')
            if not existing_name:
                continue

            # Calculate similarity ratio (normalized)
            similarity = calculate_similarity_ratio(name, existing_name)

            logger.debug(
                f"Comparing '{name}' with '{existing_name}': {similarity}% similarity"
            )

            # If similarity >= 90%, it's a duplicate
            if similarity >= SIMILARITY_THRESHOLD:
                raise ValueError(
                    f"A similar company name already exists: '{existing_name}' "
                    f"({similarity}% similar to '{name}'). "
                    f'Please use a different name or verify if this company already exists.'
                )

        logger.info('No duplicate company names found')

    except ValueError:
        # Re-raise duplicate errors
        raise
    except Exception as e:
        logger.error(f'Error checking for duplicate company: {str(e)}', exc_info=True)
        raise ValueError(f'Error checking for duplicate company: {str(e)}')


def add_new_company(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for creating a new company via HTTP POST request.

    Process:
    1. Extract and validate payload from API Gateway event
    2. Validate required fields (name, city, state, niche)
    3. Validate and format users list (or create default user)
    4. Check for duplicate company names using Levenshtein similarity (90%)
    5. Generate unique companyID with '{uuid}'
    6. Load company schema from JSON file
    7. Build complete company record with input data and None for scraping fields
    8. Insert company into DynamoDB
    9. Return success response with companyID

    Args:
        event: API Gateway HTTP event with body containing company data
        context: Lambda context object

    Returns:
        API Gateway response with 201 status and companyID on success
        API Gateway response with 400 status on validation errors
        API Gateway response with 500 status on unexpected errors

    Expected Request Body:
        {
            "name": "Clínica Auditiva São Paulo",
            "city": "São Paulo",
            "state": "SP",
            "niche": "Audiologia",
            "users": [  # Optional
                {
                    "user_name": "João Silva",
                    "job": "Gerente",
                    "status": "ativo",
                    "permission": "admin"
                }
            ]
        }

    Response Body (201 Created):
        {
            "message": "Company created successfully",
            "companyID": "{uuid}"
        }
    """
    try:
        logger.info('Processing new company creation request')

        # Extract and validate payload
        payload = extract_payload(event)
        validate_payload(payload)

        # Validate and format users
        users = validate_users(payload.get('users'))
        logger.info(f'Validated {len(users)} user(s) for company')

        # Initialize database handler
        companies_db = DatabaseHandler(table_name=settings.companies_table_name)

        # Check for duplicate company name
        check_duplicate_company(
            name=payload.get('name'),
            city=payload.get('city'),
            state=payload.get('state'),
            niche=payload.get('niche'),
            db_handler=companies_db,
        )
        logger.info('No duplicate company found, proceeding with creation')

        # Generate unique companyID
        company_id = 'company-'+str(uuid.uuid4())
        logger.info(f'Generated companyID: {company_id}')

        # Build company data using schema template
        # Load company schema template
        schema = load_company_schema()
        company_data = schema.copy()

        # Populate required fields
        company_data['companyID'] = company_id
        company_data['name'] = payload.get('name')
        company_data['city'] = payload.get('city')
        company_data['state'] = payload.get('state')
        company_data['niche'] = payload.get('niche')
        company_data['users'] = users

        # Remove empty string values to avoid storing unnecessary data in DynamoDB
        company_data = {k: v for k, v in company_data.items() if v != '' and v != {}}

        # Insert company into DynamoDB
        companies_db.insert_item(item=company_data, primary_key='companyID')

        logger.info(f'Company created successfully with ID: {company_id}')

        return response(
            status_code=201,
            body={'message': 'Company created successfully', 'companyID': company_id},
        )

    except ValueError as e:
        logger.warning(f'Validation error: {str(e)}')
        return response(status_code=400, body={'error': str(e)})

    except Exception as e:
        logger.error(f'Unexpected error creating company: {str(e)}', exc_info=True)
        return response(status_code=500, body={'error': 'Internal server error'})
