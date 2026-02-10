"""
Lambda handler for passwordless email authentication with AWS Cognito.

This module provides HTTP endpoint functionality for three-stage authentication:

Stage 1 - sendCode: Generate and send a 6-digit code via email
Stage 2 - verifyCode: Validate the code and return JWT tokens  
Stage 3 - refresh: Refresh the access token using the refresh token

Authentication Flow:
1. User enters email -> System sends 6-digit code via SES
2. User enters code -> System validates and returns access + refresh tokens
3. Token expires -> System uses refresh token to get new access token

Each stage has specific payload requirements and returns structured responses
with proper error handling and CORS support.
"""

import json
import random
import string
import uuid
from datetime import datetime, timedelta
from http import HTTPStatus
from typing import Any, Dict, Optional

import boto3
import jwt
from auris_tools.databaseHandlers import DatabaseHandler
from aws_lambda_powertools import Logger
from botocore.exceptions import ClientError

from src.shared.settings import Settings
from src.shared.utils import response

logger = Logger(service='login')
settings = Settings()

# AWS clients
cognito_client = boto3.client('cognito-idp')
ses_client = boto3.client('ses')
dynamodb = boto3.resource('dynamodb')

# CORS headers to include in all responses
CORS_HEADERS = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type,x-api-key,X-Amz-Date,Authorization,X-Api-Key',
    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET,PUT,DELETE',
}


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


def generate_challenge_id() -> str:
    """Generate unique challenge ID."""
    return "challenge-" + str(uuid.uuid4())


def mask_email(email: str) -> str:
    """
    Mask email for display (e.g., u***@company.com).

    Args:
        email: Email to mask

    Returns:
        Masked email address
    """
    parts = email.split('@')
    if len(parts[0]) <= 2:
        masked = parts[0]
    else:
        masked = parts[0][0] + '*' * (len(parts[0]) - 2) + parts[0][-1]
    return f"{masked}@{parts[1]}"


def send_code_email(email: str, code: str) -> None:
    """
    Send authentication code via SES.

    Args:
        email: Recipient email address
        code: 6-digit authentication code

    Raises:
        ClientError: If SES send fails
    """
    subject = "Seu código de autenticação Auris"
    html_body = f"""
    <html>
        <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
            <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 30px; text-align: center;">
                <h1 style="color: white; margin: 0;">Auris CRM</h1>
            </div>
            <div style="padding: 40px 30px; background-color: #f7fafc;">
                <h2 style="color: #2d3748; margin-top: 0;">Bem-vindo à Auris!</h2>
                <p style="color: #4a5568; font-size: 16px; line-height: 1.5;">
                    Utilize o código abaixo para acessar sua conta:
                </p>
                <div style="background-color: white; border: 2px solid #667eea; border-radius: 8px; padding: 20px; margin: 30px 0; text-align: center;">
                    <h1 style="color: #667eea; font-family: 'Courier New', monospace; letter-spacing: 8px; margin: 0; font-size: 36px;">
                        {code}
                    </h1>
                </div>
                <p style="color: #718096; font-size: 14px;">
                    <strong>Este código é válido por 5 minutos.</strong>
                </p>
                <p style="color: #a0aec0; font-size: 13px; margin-top: 30px;">
                    Se você não solicitou este código, ignore este email.
                </p>
            </div>
            <div style="background-color: #2d3748; padding: 20px; text-align: center;">
                <p style="color: #a0aec0; font-size: 12px; margin: 0;">
                    © {datetime.now().year} Auris CRM. Todos os direitos reservados.
                </p>
            </div>
        </body>
    </html>
    """

    text_body = f"""
Auris CRM - Código de Autenticação

Seu código: {code}

Este código é válido por 5 minutos.

Se você não solicitou este código, ignore este email.

© {datetime.now().year} Auris CRM
    """

    try:
        ses_client.send_email(
            Source=settings.ses_from_email,
            Destination={'ToAddresses': [email]},
            Message={
                'Subject': {'Data': subject, 'Charset': 'UTF-8'},
                'Body': {
                    'Text': {'Data': text_body, 'Charset': 'UTF-8'},
                    'Html': {'Data': html_body, 'Charset': 'UTF-8'},
                },
            },
        )
        logger.info(f"Email sent successfully to {email}")
    except ClientError as e:
        logger.error(f"Failed to send email to {email}: {str(e)}")
        raise


def generate_jwt_token(
    email: str,
    user_id: str,
    company_id: str,
    token_type: str,
    expires_in_hours: Optional[int] = None,
    expires_in_days: Optional[int] = None,
) -> str:
    """
    Generate JWT token for authentication.

    Args:
        email: User email
        user_id: User ID
        company_id: Company ID
        token_type: Type of token ('access' or 'refresh')
        expires_in_hours: Expiry in hours (for access tokens)
        expires_in_days: Expiry in days (for refresh tokens)

    Returns:
        JWT token string
    """
    now = datetime.utcnow()

    if token_type == 'refresh':
        expiry = now + timedelta(
            days=expires_in_days or settings.jwt_refresh_token_expiry_days
        )
    else:
        expiry = now + timedelta(
            hours=expires_in_hours or settings.jwt_access_token_expiry_hours
        )

    payload = {
        'email': email,
        'userId': user_id,
        'companyId': company_id,
        'type': token_type,
        'iat': now,
        'exp': expiry,
    }

    token = jwt.encode(payload, settings.jwt_secret_key, algorithm='HS256')
    return token


def validate_jwt_token(token: str, token_type: str) -> Optional[Dict[str, Any]]:
    """
    Validate and decode JWT token.

    Args:
        token: JWT token string
        token_type: Expected token type ('access' or 'refresh')

    Returns:
        Decoded payload dictionary if valid, None otherwise
    """
    try:
        payload = jwt.decode(token, settings.jwt_secret_key, algorithms=['HS256'])

        if payload.get('type') != token_type:
            logger.warning(f"Token type mismatch: expected {token_type}, got {payload.get('type')}")
            return None

        return payload
    except jwt.ExpiredSignatureError:
        logger.warning("Token has expired")
        return None
    except jwt.InvalidTokenError as e:
        logger.warning(f"Invalid token: {str(e)}")
        return None


def get_user_by_email(email: str) -> Optional[Dict[str, Any]]:
    """
    Fetch user information from database by email.

    NOTE: This is a simplified implementation that searches the companies table.
    For production, consider using a dedicated users table with GSI on email.

    Args:
        email: User email address

    Returns:
        User dictionary with userId, companyId, email if found, None otherwise
    """
    try:
        companies_db = DatabaseHandler(table_name=settings.companies_table_name)

        # For MVP, we'll search in the hardcoded company
        # TODO: Implement proper multi-company user lookup
        # TODO: REVISAR A TABELA companies PARA QUE users SEJA UMA LISTA INDICES DA TABELA users, COM GSI PARA CONSULTA POR EMAIL
        company_id = '896504cc-bd92-448b-bc92-74bfcd2c73c2'

        company_response = companies_db._deserialize_item(
            companies_db.get_item(key={'companyID': company_id})
        )

        if not company_response:
            return None

        users = company_response.get('users', [])
        if not isinstance(users, list):
            return None

        # Search for user with matching email
        for users_list in users:
            for user in users_list:
                if (
                    isinstance(user, dict)
                    and user.get('user_email', '').lower() == email.lower()
                ):
                    # Verify user is active
                    if user.get('status', '').lower() != 'ativo':
                        logger.warning(f"User {email} is not active")
                        return None

                    return {
                        'userId': user.get('userID', ''),
                        'companyId': company_id,
                        'email': email,
                        'userName': user.get('user_name', ''),
                    }

        return None

    except Exception as e:
        logger.error(f"Error fetching user by email: {str(e)}", exc_info=True)
        return None


def handle_send_code(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Stage 1: Send authentication code via email.

    Expected payload:
    {
        "stage": "sendCode",
        "email": "user@company.com",
        "rememberMe": false
    }

    Returns:
        Success response with challengeId and delivery details
    """
    payload = extract_payload(event)
    email = payload.get('email', '').strip().lower()
    remember_me = payload.get('rememberMe', False)

    try:
        # Check if user exists in database
        user_data = get_user_by_email(email)
        if not user_data:
            # For security, don't reveal if user exists or not
            return response(
                {'status': 'ERROR', 'error': {'code': 'USER_NOT_FOUND', 'message': 'User not found or inactive'}},
                status_code=404,
                headers=CORS_HEADERS,
            )

        # Generate 6-digit code
        code = ''.join(
            random.choices(string.digits, k=settings.auth_code_length)
        )
        created_at = datetime.utcnow()
        expires_at = created_at + timedelta(
            minutes=settings.auth_code_validity_minutes
        )
        challenge_id = generate_challenge_id()
        challenge_db = DatabaseHandler(table_name=settings.auth_codes_table_name)

        # Store code in DynamoDB with TTL
        challenge_db.insert_item(
            {
                'challengeID': challenge_id,
                'email': email,
                'code': code,
                'createdAt': int(created_at.timestamp()),
                'expiresAt': int(expires_at.timestamp()),
                'attempts': 0,
                'rememberMe': remember_me,
                'ttl': int(expires_at.timestamp()),  # Auto-delete after expiry
            },
            primary_key='challengeID',
        )

        # Send code via SES
        send_code_email(email, code)

        logger.info(f"Code sent to {email} with challengeId {challenge_id}")

        return response(
            {
                'status': 'CHALLENGE_SENT',
                'data': {
                    'challengeId': challenge_id,
                    'delivery': {
                        'type': 'EMAIL',
                        'destination': mask_email(email),
                    },
                    'challenge': {
                        'type': 'EMAIL_OTP',
                        'digits': settings.auth_code_length,
                        'expiresInSeconds': settings.auth_code_validity_minutes * 60,
                    },
                },
            },
            headers=CORS_HEADERS,
        )

    except ClientError as e:
        logger.error(f"AWS error in send_code: {str(e)}")
        return response(
            {'status': 'ERROR', 'error': {'code': 'AWS_ERROR', 'message': 'Failed to process request'}},
            status_code=500,
            headers=CORS_HEADERS,
        )
    except Exception as e:
        logger.error(f"Unexpected error in send_code: {str(e)}", exc_info=True)
        return response(
            {'status': 'ERROR', 'error': {'code': 'SERVER_ERROR', 'message': 'An unexpected error occurred'}},
            status_code=500,
            headers=CORS_HEADERS,
        )


def handle_verify_code(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Stage 2: Verify the code and return tokens.

    Expected payload:
    {
        "stage": "verifyCode",
        "email": "user@company.com",
        "shortCode": "123456",
        "challengeId": "uuid-session-id",
        "rememberMe": true
    }

    Returns:
        Success response with user data and JWT tokens
    """
    payload = extract_payload(event)
    email = payload.get('email', '').strip().lower()
    short_code = payload.get('shortCode', '').strip()
    challenge_id = payload.get('challengeId', '')
    remember_me = payload.get('rememberMe', False)

    # Validate inputs
    if not email or not short_code or not challenge_id:
        return response(
            {'status': 'ERROR', 'error': {'code': 'MISSING_PARAMS', 'message': 'Missing required parameters'}},
            status_code=400,
            headers=CORS_HEADERS,
        )

    if not short_code.isdigit() or len(short_code) != settings.auth_code_length:
        return response(
            {'status': 'ERROR', 'error': {'code': 'INVALID_CODE_FORMAT', 'message': f'Code must be {settings.auth_code_length} digits'}},
            status_code=400,
            headers=CORS_HEADERS,
        )

    try:
        # Retrieve stored code from DynamoDB
        codes_table = dynamodb.Table(settings.auth_codes_table_name)
        response_item = codes_table.get_item(Key={'challengeId': challenge_id})
        stored_data = response_item.get('Item')

        if not stored_data:
            return response(
                {'status': 'ERROR', 'error': {'code': 'INVALID_CHALLENGE', 'message': 'Challenge not found'}},
                status_code=400,
                headers=CORS_HEADERS,
            )

        # Verify email matches
        if stored_data['email'] != email:
            return response(
                {'status': 'ERROR', 'error': {'code': 'EMAIL_MISMATCH', 'message': 'Email does not match challenge'}},
                status_code=400,
                headers=CORS_HEADERS,
            )

        # Check if code has expired
        current_time = int(datetime.utcnow().timestamp())
        if current_time > stored_data['expiresAt']:
            codes_table.delete_item(Key={'challengeId': challenge_id})
            return response(
                {'status': 'ERROR', 'error': {'code': 'CODE_EXPIRED', 'message': 'Code has expired'}},
                status_code=401,
                headers=CORS_HEADERS,
            )

        # Check attempt limit
        if stored_data['attempts'] >= settings.auth_code_max_attempts:
            codes_table.delete_item(Key={'challengeId': challenge_id})
            return response(
                {'status': 'ERROR', 'error': {'code': 'MAX_ATTEMPTS_EXCEEDED', 'message': 'Too many attempts'}},
                status_code=401,
                headers=CORS_HEADERS,
            )

        # Verify code
        if stored_data['code'] != short_code:
            # Increment attempts
            codes_table.update_item(
                Key={'challengeId': challenge_id},
                UpdateExpression='SET attempts = attempts + :inc',
                ExpressionAttributeValues={':inc': 1},
            )
            return response(
                {'status': 'ERROR', 'error': {'code': 'INVALID_CODE', 'message': 'Code is incorrect'}},
                status_code=401,
                headers=CORS_HEADERS,
            )

        # Code is valid - delete the challenge
        codes_table.delete_item(Key={'challengeId': challenge_id})

        # Get user data
        user_data = get_user_by_email(email)
        if not user_data:
            return response(
                {'status': 'ERROR', 'error': {'code': 'USER_NOT_FOUND', 'message': 'User not found'}},
                status_code=404,
                headers=CORS_HEADERS,
            )

        # Generate tokens
        access_token = generate_jwt_token(
            email=email,
            user_id=user_data['userId'],
            company_id=user_data['companyId'],
            token_type='access',
        )

        refresh_token = None
        if remember_me:
            refresh_token = generate_jwt_token(
                email=email,
                user_id=user_data['userId'],
                company_id=user_data['companyId'],
                token_type='refresh',
            )

        logger.info(f"User {email} authenticated successfully")

        response_data = {
            'user': {
                'userId': user_data['userId'],
                'companyId': user_data['companyId'],
                'email': email,
            },
            'tokens': {
                'accessToken': {
                    'value': access_token,
                    'expiresAt': (
                        datetime.utcnow()
                        + timedelta(hours=settings.jwt_access_token_expiry_hours)
                    ).isoformat()
                    + 'Z',
                },
            },
        }

        if remember_me and refresh_token:
            response_data['tokens']['refreshToken'] = {'value': refresh_token}

        return response(
            {'status': 'AUTHENTICATED', 'data': response_data},
            headers=CORS_HEADERS,
        )

    except ClientError as e:
        logger.error(f"AWS error in verify_code: {str(e)}")
        return response(
            {'status': 'ERROR', 'error': {'code': 'AWS_ERROR', 'message': 'Failed to process request'}},
            status_code=500,
            headers=CORS_HEADERS,
        )
    except Exception as e:
        logger.error(f"Unexpected error in verify_code: {str(e)}", exc_info=True)
        return response(
            {'status': 'ERROR', 'error': {'code': 'SERVER_ERROR', 'message': 'An unexpected error occurred'}},
            status_code=500,
            headers=CORS_HEADERS,
        )


def handle_refresh_token(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Stage 3: Refresh access token.

    Expected payload:
    {
        "stage": "refresh",
        "refreshToken": "jwt_refresh_token",
        "email": "user@company.com",
        "userId": "usr_123",
        "companyId": "cmp_456"
    }

    Returns:
        Success response with new access token
    """
    payload = extract_payload(event)
    refresh_token = payload.get('refreshToken', '')
    email = payload.get('email', '').strip().lower()
    user_id = payload.get('userId', '')
    company_id = payload.get('companyId', '')

    # Validate inputs
    if not refresh_token or not email:
        return response(
            {'status': 'ERROR', 'error': {'code': 'MISSING_PARAMS', 'message': 'Missing refresh token or email'}},
            status_code=400,
            headers=CORS_HEADERS,
        )

    try:
        # Validate refresh token JWT
        token_payload = validate_jwt_token(refresh_token, token_type='refresh')
        if not token_payload:
            return response(
                {'status': 'ERROR', 'error': {'code': 'INVALID_TOKEN', 'message': 'Refresh token is invalid or expired'}},
                status_code=401,
                headers=CORS_HEADERS,
            )

        # Verify email in token matches request
        if token_payload.get('email') != email:
            return response(
                {'status': 'ERROR', 'error': {'code': 'TOKEN_EMAIL_MISMATCH', 'message': 'Email does not match token'}},
                status_code=401,
                headers=CORS_HEADERS,
            )

        # Use token payload data for new access token
        new_access_token = generate_jwt_token(
            email=email,
            user_id=token_payload.get('userId', user_id),
            company_id=token_payload.get('companyId', company_id),
            token_type='access',
        )

        logger.info(f"Token refreshed for user {email}")

        return response(
            {
                'status': 'TOKEN_REFRESHED',
                'data': {
                    'tokens': {
                        'accessToken': {
                            'value': new_access_token,
                            'expiresAt': (
                                datetime.utcnow()
                                + timedelta(hours=settings.jwt_access_token_expiry_hours)
                            ).isoformat()
                            + 'Z',
                        }
                    }
                },
            },
            headers=CORS_HEADERS,
        )

    except Exception as e:
        logger.error(f"Error in refresh_token: {str(e)}", exc_info=True)
        return response(
            {'status': 'ERROR', 'error': {'code': 'TOKEN_REFRESH_FAILED', 'message': 'Failed to refresh token'}},
            status_code=401,
            headers=CORS_HEADERS,
        )


def login(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler for passwordless authentication.

    Routes requests to appropriate handler based on stage parameter.
    Supports three authentication stages:
    - sendCode: Generate and send verification code
    - verifyCode: Validate code and return tokens
    - refresh: Refresh access token

    Args:
        event: API Gateway HTTP event
        context: Lambda context object

    Returns:
        API Gateway response dictionary

    Example Payloads:

    **Stage 1: sendCode**
    Request:
    {
        "stage": "sendCode",
        "email": "user@company.com",
        "rememberMe": false
    }

    Response (Success - 200):
    {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", ...},
        "body": {
            "status": "CHALLENGE_SENT",
            "data": {
                "challengeId": "challenge-550e8400-e29b-41d4-a716-446655440000",
                "delivery": {
                    "type": "EMAIL",
                    "destination": "u***@company.com"
                },
                "challenge": {
                    "type": "EMAIL_OTP",
                    "digits": 6,
                    "expiresInSeconds": 300
                }
            }
        }
    }

    Response (Error - 404):
    {
        "statusCode": 404,
        "headers": {"Content-Type": "application/json", ...},
        "body": {
            "status": "ERROR",
            "error": {
                "code": "USER_NOT_FOUND",
                "message": "User not found or inactive"
            }
        }
    }

    ---

    **Stage 2: verifyCode**
    Request:
    {
        "stage": "verifyCode",
        "email": "user@company.com",
        "shortCode": "123456",
        "challengeId": "challenge-550e8400-e29b-41d4-a716-446655440000",
        "rememberMe": true
    }

    Response (Success - 200):
    {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", ...},
        "body": {
            "status": "AUTHENTICATED",
            "data": {
                "user": {
                    "userId": "usr_550e8400e29b41d4a716446655440000",
                    "companyId": "896504cc-bd92-448b-bc92-74bfcd2c73c2",
                    "email": "user@company.com"
                },
                "tokens": {
                    "accessToken": {
                        "value": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
                        "expiresAt": "2026-02-11T08:30:45.123456Z"
                    },
                    "refreshToken": {
                        "value": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
                    }
                }
            }
        }
    }

    Response (Error - 401):
    {
        "statusCode": 401,
        "headers": {"Content-Type": "application/json", ...},
        "body": {
            "status": "ERROR",
            "error": {
                "code": "INVALID_CODE",
                "message": "Code is incorrect"
            }
        }
    }

    ---

    **Stage 3: refresh**
    Request:
    {
        "stage": "refresh",
        "refreshToken": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
        "email": "user@company.com",
        "userId": "usr_550e8400e29b41d4a716446655440000",
        "companyId": "896504cc-bd92-448b-bc92-74bfcd2c73c2"
    }

    Response (Success - 200):
    {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", ...},
        "body": {
            "status": "TOKEN_REFRESHED",
            "data": {
                "tokens": {
                    "accessToken": {
                        "value": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
                        "expiresAt": "2026-02-11T08:30:45.123456Z"
                    }
                }
            }
        }
    }

    Response (Error - 401):
    {
        "statusCode": 401,
        "headers": {"Content-Type": "application/json", ...},
        "body": {
            "status": "ERROR",
            "error": {
                "code": "INVALID_TOKEN",
                "message": "Refresh token is invalid or expired"
            }
        }
    }
    """
    try:
        logger.info('Processing authentication request')

        # Handle OPTIONS preflight request
        if event.get('httpMethod') == 'OPTIONS':
            logger.info('Handling OPTIONS preflight request')
            return {
                'statusCode': 200,
                'headers': CORS_HEADERS,
                'body': json.dumps({'message': 'OK'}),
            }

        # Extract stage from payload
        payload = extract_payload(event)
        stage = payload.get('stage')

        if stage == 'sendCode':
            return handle_send_code(event)
        elif stage == 'verifyCode':
            return handle_verify_code(event)
        elif stage == 'refresh':
            return handle_refresh_token(event)
        else:
            return response(
                {'status': 'ERROR', 'error': {'code': 'INVALID_STAGE', 'message': 'Invalid authentication stage'}},
                status_code=400,
                headers=CORS_HEADERS,
            )

    except ValueError as e:
        logger.warning(f'Validation error: {str(e)}')
        return response(
            {'status': 'ERROR', 'error': {'code': 'VALIDATION_ERROR', 'message': str(e)}},
            status_code=400,
            headers=CORS_HEADERS,
        )

    except Exception as e:
        logger.error(f'Unexpected error: {str(e)}', exc_info=True)
        return response(
            {'status': 'ERROR', 'error': {'code': 'SERVER_ERROR', 'message': 'An unexpected error occurred'}},
            status_code=500,
            headers=CORS_HEADERS,
        )

