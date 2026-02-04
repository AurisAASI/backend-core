"""
Lambda handler for user authentication in the Auris CRM system.

This module provides HTTP endpoint functionality for user login with:
- Email-only validation (password removed per requirements)
- API Gateway event payload extraction and validation
- Single company user lookup (hardcoded for MVP testing)
- User status validation (only 'ativo' users allowed)
- CORS configuration matching other endpoints
- Cognito integration roadmap with helper stubs

TODO-COGNITO: When Cognito is integrated, replace email-based database lookup
with JWT token validation from Authorization header. See _parse_authorization_header()
and _validate_jwt_token() stub functions below for integration points.

TODO-PERFORMANCE: For multi-company support, replace hardcoded companyID with:
- Option B: Table scan with FilterExpression (interim)
- Option A: Dedicated users table with GSI on email (final)
See architecture roadmap for details.
"""

import json
import re
from http import HTTPStatus
from typing import Any, Dict, Optional

from auris_tools.databaseHandlers import DatabaseHandler
from aws_lambda_powertools import Logger

from src.shared.settings import Settings
from src.shared.utils import response

logger = Logger(service='login')
settings = Settings()

# CORS headers to include in all responses
CORS_HEADERS = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type,x-api-key,X-Amz-Date,Authorization,X-Api-Key',
    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET,PUT,DELETE',
}

# TODO-MIGRATION: Replace with Cognito claims extraction when authentication refactored
# For MVP testing, using hardcoded single company
HARDCODED_COMPANY_ID = '896504cc-bd92-448b-bc92-74bfcd2c73c2'


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


def validate_email(email: str) -> None:
    """
    Validate email format and prevent malicious inputs.

    Args:
        email: Email address to validate

    Raises:
        ValueError: If email is invalid or empty
    """
    if not email:
        raise ValueError('Email is required')

    # Basic email format validation (RFC 5322 simplified)
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if not re.match(email_pattern, email):
        raise ValueError('Invalid email format')

    # Prevent extremely long emails (potential injection)
    if len(email) > 254:
        raise ValueError('Email is too long')


def _parse_authorization_header(event: Dict[str, Any]) -> Optional[str]:
    """
    Extract JWT token from Authorization header.

    TODO-COGNITO: This stub function will be expanded to parse and validate
    JWT tokens when Cognito integration is implemented. Currently accepts
    Authorization header but does not validate.

    Args:
        event: API Gateway event containing headers

    Returns:
        Authorization header value or None if not present
    """
    headers = event.get('headers', {})
    auth_header = headers.get('Authorization') or headers.get('authorization')
    if auth_header:
        logger.info('Authorization header present (validation deferred to Cognito integration)')
    return auth_header


def _validate_jwt_token(token: str) -> Dict[str, Any]:
    """
    Validate JWT token and extract claims.

    TODO-COGNITO: Implement JWT token validation using Cognito public keys.
    This should:
    1. Verify token signature against Cognito public keys
    2. Check token expiration
    3. Extract claims containing companyID and userID
    4. Return claims dictionary

    Current implementation: stub returning empty dict (no validation)

    Args:
        token: JWT token from Authorization header

    Returns:
        Claims dictionary containing companyID, userID, and other info

    Raises:
        ValueError: If token is invalid or expired
    """
    # TODO-COGNITO: Implement actual JWT validation here
    logger.warning('JWT token validation not yet implemented (Cognito integration pending)')
    return {}


def find_user_by_email(
    company_id: str, email: str, companies_db: DatabaseHandler
) -> Optional[Dict[str, Any]]:
    """
    Search for user by email in company's users array.

    Searches the hardcoded/single company's nested users list for matching email.

    TODO-PERFORMANCE: For multi-company support, this logic should be replaced with:
    - Option B: Table scan with FilterExpression checking all companies
    - Option A: Dedicated users GSI table for O(1) email lookup

    Args:
        company_id: Company ID to search in
        email: Email to match
        companies_db: DatabaseHandler for companies table

    Returns:
        Dictionary with {companyID, userID, user_name, status, permission} if found, None otherwise
    """
    try:
        logger.info(f'Searching for user with email: {email} in company: {company_id}')

        # Fetch the company record
        company_response = companies_db._deserialize_item(companies_db.get_item(key={'companyID': company_id}))

        if not company_response:
            logger.warning(f'Company not found: {company_id}')
            return None

        # Extract users list from company record
        users = company_response.get('users', [])
        if not isinstance(users, list):
            logger.error(f'Invalid users structure in company {company_id} - users: {users}')
            return None

        # Search for user with matching email
        for users_list in users:
            for user in users_list:
                if isinstance(user, dict) and user.get('user_email', '').lower() == email.lower():
                    logger.info(f'User found with email: {email}')
                    return {**user, 'companyID': company_id}

        logger.warning(f'User not found with email: {email} in company: {company_id}')
        return None

    except Exception as e:
        logger.error(f'Error searching for user: {str(e)}', exc_info=True)
        raise


def login(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for user authentication via email lookup.

    Process:
    1. Handle OPTIONS preflight request
    2. Extract and validate email from request body
    3. Query company's users for matching email
    4. Validate user status is 'ativo'
    5. Return success with companyID and userID

    Args:
        event: API Gateway HTTP event with body containing email
        context: Lambda context object

    Returns:
        API Gateway response with 200 status and user details on success
        API Gateway response with 401 status if user not found or inactive
        API Gateway response with 400 status on validation errors
        API Gateway response with 500 status on unexpected errors

    Expected Request Body:
        {
            "email": "user@example.com"
        }

    Response Body (200 OK):
        {
            "success": true,
            "companyID": "COMP001",
            "userID": "user-uuid-here"
        }

    Response Body (401 Unauthorized):
        {
            "success": false,
            "message": "User not found" | "User account is inactive"
        }
    """
    try:
        logger.info('Processing login request')

        # Handle OPTIONS preflight request
        if event.get('httpMethod') == 'OPTIONS':
            logger.info('Handling OPTIONS preflight request')
            return response(status_code=200, message='', headers=CORS_HEADERS)

        # Extract and validate payload
        payload = extract_payload(event)
        email = payload.get('email', '').strip()

        validate_email(email)
        logger.info(f'Email validated: {email}')

        # TODO-COGNITO: When Cognito is integrated, parse Authorization header
        # and validate JWT token instead of email lookup
        auth_header = _parse_authorization_header(event)
        if auth_header:
            logger.info('Authorization header detected (Cognito integration pending)')
            # TODO-COGNITO: Uncomment and implement when ready:
            # claims = _validate_jwt_token(auth_header)
            # company_id = claims.get('companyID')
            # user_id = claims.get('userID')

        # Initialize database handler
        companies_db = DatabaseHandler(table_name=settings.companies_table_name)

        # Search for user by email in hardcoded company
        user_data = find_user_by_email(HARDCODED_COMPANY_ID, email, companies_db)

        if not user_data:
            logger.warning(f'User not found: {email}')
            return response(
                status_code=404,
                message={'success': False, 'message': 'User not found'},
                headers=CORS_HEADERS,
            )

        # Validate user status is 'ativo'
        user_status = user_data.get('status', '').lower()
        if user_status != 'ativo':
            logger.warning(
                f'Login attempt with inactive user: {email} (status: {user_status})'
            )
            return response(
                status_code=401,
                message={
                    'success': False,
                    'message': 'User account is inactive',
                },
                headers=CORS_HEADERS,
            )

        logger.info(
            f'User authenticated successfully: {email} (companyID: {user_data["companyID"]}, userID: {user_data["userID"]})'
        )

        return response(
            status_code=200,
            message={
                'success': True,
                'companyID': user_data['companyID'],
                'userID': user_data['userID'],
            },
            headers=CORS_HEADERS,
        )

    except ValueError as e:
        logger.warning(f'Validation error: {str(e)}')
        return response(
            status_code=400,
            message={'success': False, 'message': str(e)},
            headers=CORS_HEADERS,
        )

    except Exception as e:
        logger.error(f'Unexpected error during login: {str(e)}', exc_info=True)
        return response(
            status_code=500,
            message={'success': False, 'message': 'Internal server error'},
            headers=CORS_HEADERS,
        )
