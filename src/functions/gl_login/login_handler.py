"""
Lambda handler for passwordless email authentication with AWS Cognito.

This module provides HTTP endpoint functionality for three-stage authentication:

Stage 1 - sendCode: Generate and send a 6-digit code via email
    Stage 2 - verifyCode: Validate the code and return Cognito tokens
    Stage 3 - refresh: Refresh the access token using the refresh token

Authentication Flow:
1. User enters email -> System sends 6-digit code via SES
2. User enters code -> System validates and returns access + refresh tokens
3. Token expires -> System uses refresh token to get new access token from Cognito

Each stage has specific payload requirements and returns structured responses
with proper error handling and CORS support.
"""

import base64
import hashlib
import hmac
import json
import random
import string
import uuid
from datetime import datetime, timedelta
from http import HTTPStatus
from typing import Any, Dict, Optional

import boto3
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

auth_codes_db = DatabaseHandler(table_name=settings.auth_codes_table_name)

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
    return 'challenge-' + str(uuid.uuid4())


def get_cognito_user_pool_id() -> str:
    """Get Cognito user pool ID for the current stage."""
    return settings.cognito_user_pool_id


def get_cognito_app_client_id() -> str:
    """Get Cognito app client ID for the current stage."""
    if settings.stage == 'prod':
        return settings.cognito_app_client_id_prod
    return settings.cognito_app_client_id_dev


def get_cognito_app_client_secret() -> Optional[str]:
    """Get Cognito app client secret for the current stage, if configured."""
    if settings.stage == 'prod':
        return settings.cognito_app_client_secret_prod or None
    return settings.cognito_app_client_secret_dev or None


def build_secret_hash(
    username: str, client_id: str, client_secret: Optional[str]
) -> Optional[str]:
    """Build Cognito secret hash when app client secret is configured."""
    if not client_secret:
        return None
    message = f'{username}{client_id}'.encode('utf-8')
    digest = hmac.new(client_secret.encode('utf-8'), message, hashlib.sha256).digest()
    return base64.b64encode(digest).decode('utf-8')


def build_auth_parameters(username: str, client_id: str) -> Dict[str, str]:
    """Build Cognito auth parameters with optional secret hash."""
    params = {'USERNAME': username}
    secret = get_cognito_app_client_secret()
    secret_hash = build_secret_hash(username, client_id, secret)
    if secret_hash:
        params['SECRET_HASH'] = secret_hash
    return params


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
    return f'{masked}@{parts[1]}'


def send_code_email(email: str, code: str) -> None:
    """
    Send authentication code via SES.

    Args:
        email: Recipient email address
        code: 6-digit authentication code

    Raises:
        ClientError: If SES send fails
    """
    subject = 'Seu código de autenticação Auris'
    html_body = f"""
    <!DOCTYPE html>
    <html lang="pt-BR">
        <head>
            <meta charset="utf-8" />
            <meta content="width=device-width, initial-scale=1.0" name="viewport" />
            <title>Codigo de Acesso - Auris Saude</title>
        </head>
        <body style="margin: 0; padding: 0; background-color: #f3f4f6; color: #1f2937; font-family: Arial, Helvetica, sans-serif;">
            <div style="padding: 24px 16px;">
                <div style="max-width: 480px; margin: 0 auto;">
                    <div style="background-color: #ffffff; border: 1px solid #f1f5f9; border-radius: 16px; overflow: hidden; box-shadow: 0 12px 24px rgba(15, 23, 42, 0.08);">
                        <div style="padding: 32px 32px 0 32px; text-align: center;">
                            <div style="display: inline-flex; align-items: center; gap: 10px; margin-bottom: 28px;">
                                <div style="height: 40px; width: 40px; border-radius: 999px; background-color: rgba(56, 178, 172, 0.12); display: inline-flex; align-items: center; justify-content: center; color: #38b2ac; font-weight: 700;">
                                    A
                                </div>
                                <span style="font-size: 20px; font-weight: 700; letter-spacing: -0.02em; color: #1f2937;">Auris Saude</span>
                            </div>
                        </div>
                        <div style="padding: 0 32px 32px 32px; text-align: center;">
                            <h1 style="font-size: 22px; font-weight: 700; color: #111827; margin: 0 0 12px 0;">Seu código de acesso chegou</h1>
                            <p style="color: #6b7280; font-size: 14px; line-height: 1.6; margin: 0 0 24px 0;">
                                Utilize o código abaixo para validar sua identidade e acessar o CRM da Auris Saude.
                            </p>
                            <div style="background-color: #38b2ac; border-radius: 12px; padding: 18px 12px; margin: 0 0 24px 0;">
                                <span style="display: inline-block; font-family: 'Courier New', Courier, monospace; font-size: 32px; font-weight: 700; letter-spacing: 0.4em; color: #ffffff; padding-left: 0.2em;">{code}</span>
                            </div>
                            <p style="font-size: 14px; font-weight: 600; color: #374151; margin: 0 0 12px 0;">
                                Este código é válido por <span style="color: #2c7a7b;">{settings.auth_code_validity_minutes} minutos</span>.
                            </p>
                            <div style="height: 1px; background-color: #f1f5f9; width: 100%; margin: 12px 0 16px 0;"></div>
                            <p style="font-size: 12px; color: #6b7280; line-height: 1.6; font-style: italic; margin: 0;">
                                Se você não solicitou este acesso, por favor ignore este email. Nenhuma ação adicional é necessária.
                            </p>
                        </div>
                        <div style="height: 6px; background: linear-gradient(90deg, #38b2ac 0%, #2c7a7b 100%);"></div>
                    </div>
                    <div style="margin-top: 24px; text-align: center; padding: 0 8px;">
                        <p style="font-size: 12px; color: #6b7280; font-weight: 600; margin: 0 0 6px 0;">
                            Auris Saude Auditiva &amp; Clinical Management
                        </p>
                        <p style="font-size: 10px; color: #9ca3af; text-transform: uppercase; letter-spacing: 0.2em; margin: 0;">
                            &copy; {datetime.now().year} Auris Saude. Todos os direitos reservados.
                        </p>
                    </div>
                </div>
            </div>
        </body>
    </html>
    """

    text_body = f"""
Auris Saude - Código de Acesso

Seu código: {code}

Este código é válido por {settings.auth_code_validity_minutes} minutos.

Se você não solicitou este acesso, ignore este email.

© {datetime.now().year} Auris Saude. Todos os direitos reservados.
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
        logger.info(f'Email sent successfully to {email}')
    except ClientError as e:
        logger.error(f'Failed to send email to {email}: {str(e)}')
        raise


def ensure_cognito_user(email: str, user_name: str) -> str:
    """
    Ensure a Cognito user exists and return the Cognito username.

    Args:
        email: User email address
        user_name: User display name

    Returns:
        Cognito username
    """
    user_pool_id = get_cognito_user_pool_id()
    response_data = cognito_client.list_users(
        UserPoolId=user_pool_id,
        Filter=f'email = "{email}"',
        Limit=1,
    )

    if response_data.get('Users'):
        user = response_data['Users'][0]
        username = user['Username']
        if not user.get('Enabled', True):
            cognito_client.admin_enable_user(UserPoolId=user_pool_id, Username=username)

        attributes = {
            attr['Name']: attr['Value'] for attr in user.get('Attributes', [])
        }
        if attributes.get('email_verified') != 'true':
            cognito_client.admin_update_user_attributes(
                UserPoolId=user_pool_id,
                Username=username,
                UserAttributes=[{'Name': 'email_verified', 'Value': 'true'}],
            )

        return username

    user_attributes = [
        {'Name': 'email', 'Value': email},
        {'Name': 'email_verified', 'Value': 'true'},
    ]
    if user_name:
        user_attributes.append({'Name': 'name', 'Value': user_name})

    create_response = cognito_client.admin_create_user(
        UserPoolId=user_pool_id,
        Username=email,
        UserAttributes=user_attributes,
        MessageAction='SUPPRESS',
    )
    username = create_response['User']['Username']

    try:
        cognito_client.admin_confirm_sign_up(
            UserPoolId=user_pool_id,
            Username=username,
        )
    except ClientError as e:
        if e.response.get('Error', {}).get('Code') != 'NotAuthorizedException':
            logger.warning(f'Failed to confirm Cognito user {email}: {str(e)}')

    return username


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
                        logger.warning(f'User {email} is not active')
                        return None

                    return {
                        'userId': user.get('userID', ''),
                        'companyId': company_id,
                        'email': email,
                        'userName': user.get('user_name', ''),
                    }

        return None

    except Exception as e:
        logger.error(f'Error fetching user by email: {str(e)}', exc_info=True)
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
                {
                    'status': 'ERROR',
                    'error': {
                        'code': 'USER_NOT_FOUND',
                        'message': 'User not found or inactive',
                    },
                },
                status_code=404,
                headers=CORS_HEADERS,
            )

        challenge_id = generate_challenge_id()

        # Ensure Cognito user exists and initiate custom auth
        cognito_username = ensure_cognito_user(email, user_data.get('userName', ''))
        client_id = get_cognito_app_client_id()
        auth_params = build_auth_parameters(cognito_username, client_id)

        auth_response = cognito_client.initiate_auth(
            ClientId=client_id,
            AuthFlow='CUSTOM_AUTH',
            AuthParameters=auth_params,
            ClientMetadata={
                'challengeId': challenge_id,
                'email': email,
            },
        )

        if auth_response.get('ChallengeName') != 'CUSTOM_CHALLENGE':
            logger.error(
                f"Unexpected challenge name: {auth_response.get('ChallengeName')}"
            )
            return response(
                {
                    'status': 'ERROR',
                    'error': {
                        'code': 'AUTH_CHALLENGE_ERROR',
                        'message': 'Unexpected authentication challenge',
                    },
                },
                status_code=500,
                headers=CORS_HEADERS,
            )

        session_token = auth_response.get('Session')
        if not session_token:
            logger.error('Cognito did not return a session token')
            return response(
                {
                    'status': 'ERROR',
                    'error': {
                        'code': 'AUTH_SESSION_ERROR',
                        'message': 'Failed to initiate authentication session',
                    },
                },
                status_code=500,
                headers=CORS_HEADERS,
            )

        # Generate 6-digit code
        code = ''.join(random.choices(string.digits, k=settings.auth_code_length))
        created_at = datetime.utcnow()
        expires_at = created_at + timedelta(minutes=settings.auth_code_validity_minutes)

        # Store code in DynamoDB with TTL
        auth_codes_db.insert_item(
            item={
                'challengeID': challenge_id,
                'email': email,
                'code': code,
                'createdAt': int(created_at.timestamp()),
                'expiresAt': int(expires_at.timestamp()),
                'attempts': 0,
                'rememberMe': remember_me,
                'cognitoSession': session_token,
                'cognitoUsername': cognito_username,
                'ttl': int(expires_at.timestamp()),
            },
            primary_key='challengeID',
        )

        # Send code via SES
        send_code_email(email, code)

        logger.info(f'Code sent to {email} with challengeId {challenge_id}')

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
        logger.error(f'AWS error in send_code: {str(e)}')
        return response(
            {
                'status': 'ERROR',
                'error': {'code': 'AWS_ERROR', 'message': 'Failed to process request'},
            },
            status_code=500,
            headers=CORS_HEADERS,
        )
    except Exception as e:
        logger.error(f'Unexpected error in send_code: {str(e)}', exc_info=True)
        return response(
            {
                'status': 'ERROR',
                'error': {
                    'code': 'SERVER_ERROR',
                    'message': 'An unexpected error occurred',
                },
            },
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
        Success response with user data and Cognito tokens
    """
    payload = extract_payload(event)
    email = payload.get('email', '').strip().lower()
    short_code = (payload.get('shortCode') or payload.get('code') or '').strip()
    challenge_id = payload.get('challengeId') or payload.get('challengeID', '')
    remember_me = payload.get('rememberMe', False)

    # Validate inputs
    if not email or not short_code or not challenge_id:
        return response(
            {
                'status': 'ERROR',
                'error': {
                    'code': 'MISSING_PARAMS',
                    'message': 'Missing required parameters',
                },
            },
            status_code=400,
            headers=CORS_HEADERS,
        )

    if not short_code.isdigit() or len(short_code) != settings.auth_code_length:
        return response(
            {
                'status': 'ERROR',
                'error': {
                    'code': 'INVALID_CODE_FORMAT',
                    'message': f'Code must be {settings.auth_code_length} digits',
                },
            },
            status_code=400,
            headers=CORS_HEADERS,
        )

    try:
        # Retrieve stored code from DynamoDB
        stored_data = auth_codes_db._deserialize_item(
            auth_codes_db.get_item(key={'challengeID': challenge_id})
        )

        if not stored_data:
            logger.warning(f'Challenge ID {challenge_id} not found')
            return response(
                {
                    'status': 'ERROR',
                    'error': {
                        'code': 'INVALID_CHALLENGE',
                        'message': 'Challenge not found',
                    },
                },
                status_code=400,
                headers=CORS_HEADERS,
            )

        # Verify email matches
        if stored_data['email'] != email:
            logger.warning(
                f"Email mismatch for challenge ID {challenge_id}: expected {stored_data['email']}, got {email}"
            )
            return response(
                {
                    'status': 'ERROR',
                    'error': {
                        'code': 'EMAIL_MISMATCH',
                        'message': 'Email does not match challenge',
                    },
                },
                status_code=400,
                headers=CORS_HEADERS,
            )

        if 'rememberMe' not in payload:
            remember_me = stored_data.get('rememberMe', False)

        # Check if code has expired
        current_time = int(datetime.utcnow().timestamp())
        if current_time > stored_data['expiresAt']:
            logger.warning(f'Code expired for challenge ID {challenge_id}')
            auth_codes_db.delete_item(
                key={'challengeID': challenge_id},
                primary_key='challengeID',
            )
            return response(
                {
                    'status': 'ERROR',
                    'error': {'code': 'CODE_EXPIRED', 'message': 'Code has expired'},
                },
                status_code=401,
                headers=CORS_HEADERS,
            )

        # Check attempt limit
        if stored_data['attempts'] >= settings.auth_code_max_attempts:
            logger.warning(f'Max attempts exceeded for challenge ID {challenge_id}')
            auth_codes_db.delete_item(
                key={'challengeID': challenge_id},
                primary_key='challengeID',
            )
            return response(
                {
                    'status': 'ERROR',
                    'error': {
                        'code': 'MAX_ATTEMPTS_EXCEEDED',
                        'message': 'Too many attempts',
                    },
                },
                status_code=401,
                headers=CORS_HEADERS,
            )

        # Verify code
        if stored_data['code'] != short_code:
            logger.warning(
                f"Invalid code for challenge ID {challenge_id}: expected {stored_data['code']}, got {short_code}"
            )
            # Increment attempts
            auth_codes_db.update_item(
                key={'challengeID': challenge_id},
                updates={'attempts': stored_data['attempts'] + 1},
                primary_key='challengeID',
            )
            return response(
                {
                    'status': 'ERROR',
                    'error': {'code': 'INVALID_CODE', 'message': 'Code is incorrect'},
                },
                status_code=401,
                headers=CORS_HEADERS,
            )

        # Code is valid - exchange for Cognito tokens
        cognito_session = stored_data.get('cognitoSession')
        cognito_username = stored_data.get('cognitoUsername') or email
        if not cognito_session:
            logger.error(f'Missing Cognito session for challenge ID {challenge_id}')
            return response(
                {
                    'status': 'ERROR',
                    'error': {
                        'code': 'AUTH_SESSION_MISSING',
                        'message': 'Authentication session not found',
                    },
                },
                status_code=401,
                headers=CORS_HEADERS,
            )

        client_id = get_cognito_app_client_id()
        challenge_responses = {
            'USERNAME': cognito_username,
            'ANSWER': short_code,
        }
        secret = get_cognito_app_client_secret()
        secret_hash = build_secret_hash(cognito_username, client_id, secret)
        if secret_hash:
            challenge_responses['SECRET_HASH'] = secret_hash

        auth_response = cognito_client.respond_to_auth_challenge(
            ClientId=client_id,
            ChallengeName='CUSTOM_CHALLENGE',
            Session=cognito_session,
            ChallengeResponses=challenge_responses,
            ClientMetadata={
                'challengeId': challenge_id,
                'email': email,
            },
        )

        auth_result = auth_response.get('AuthenticationResult')
        if not auth_result:
            logger.error(f'Cognito did not return tokens for {email}')
            return response(
                {
                    'status': 'ERROR',
                    'error': {
                        'code': 'TOKEN_ISSUE_FAILED',
                        'message': 'Failed to issue tokens',
                    },
                },
                status_code=500,
                headers=CORS_HEADERS,
            )

        # Code is valid - delete the challenge
        logger.info(
            f'Code verified successfully for challenge ID {challenge_id} and email {email}'
        )
        auth_codes_db.delete_item(
            key={'challengeID': challenge_id},
            primary_key='challengeID',
        )

        # Get user data
        user_data = get_user_by_email(email)
        if not user_data:
            logger.warning(f'User not found for email {email}')
            return response(
                {
                    'status': 'ERROR',
                    'error': {'code': 'USER_NOT_FOUND', 'message': 'User not found'},
                },
                status_code=404,
                headers=CORS_HEADERS,
            )

        access_token = auth_result.get('AccessToken')
        id_token = auth_result.get('IdToken')
        refresh_token = auth_result.get('RefreshToken')
        expires_in = auth_result.get('ExpiresIn') or 3600
        expires_at = (
            datetime.utcnow() + timedelta(seconds=expires_in)
        ).isoformat() + 'Z'

        logger.info(f'User {email} authenticated successfully')

        response_data = {
            'user': {
                'userId': user_data['userId'],
                'companyId': user_data['companyId'],
                'email': email,
            },
            'tokens': {
                'accessToken': {
                    'value': access_token,
                    'expiresAt': expires_at,
                },
                'idToken': {'value': id_token},
            },
        }

        if remember_me and refresh_token:
            response_data['tokens']['refreshToken'] = {'value': refresh_token}

        return response(
            {'status': 'AUTHENTICATED', 'data': response_data},
            headers=CORS_HEADERS,
        )

    except ClientError as e:
        logger.error(f'AWS error in verify_code: {str(e)}')
        return response(
            {
                'status': 'ERROR',
                'error': {'code': 'AWS_ERROR', 'message': 'Failed to process request'},
            },
            status_code=500,
            headers=CORS_HEADERS,
        )
    except Exception as e:
        logger.error(f'Unexpected error in verify_code: {str(e)}', exc_info=True)
        return response(
            {
                'status': 'ERROR',
                'error': {
                    'code': 'SERVER_ERROR',
                    'message': 'An unexpected error occurred',
                },
            },
            status_code=500,
            headers=CORS_HEADERS,
        )


def handle_refresh_token(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Stage 3: Refresh access token.

    Expected payload:
    {
        "stage": "refresh",
        "refreshToken": "cognito_refresh_token",
        "email": "user@company.com"
    }

    Returns:
        Success response with new access token
    """
    payload = extract_payload(event)
    refresh_token = payload.get('refreshToken', '')
    email = payload.get('email', '').strip().lower()

    # Validate inputs
    if not refresh_token:
        logger.warning('Missing refresh token in refresh request')
        return response(
            {
                'status': 'ERROR',
                'error': {'code': 'MISSING_PARAMS', 'message': 'Missing refresh token'},
            },
            status_code=400,
            headers=CORS_HEADERS,
        )

    try:
        client_id = get_cognito_app_client_id()
        secret = get_cognito_app_client_secret()
        auth_parameters = {'REFRESH_TOKEN': refresh_token}

        if secret:
            if not email:
                return response(
                    {
                        'status': 'ERROR',
                        'error': {
                            'code': 'MISSING_EMAIL',
                            'message': 'Email is required to refresh token',
                        },
                    },
                    status_code=400,
                    headers=CORS_HEADERS,
                )
            auth_parameters['USERNAME'] = email
            auth_parameters['SECRET_HASH'] = build_secret_hash(email, client_id, secret)

        auth_response = cognito_client.initiate_auth(
            ClientId=client_id,
            AuthFlow='REFRESH_TOKEN_AUTH',
            AuthParameters=auth_parameters,
        )

        auth_result = auth_response.get('AuthenticationResult')
        if not auth_result:
            return response(
                {
                    'status': 'ERROR',
                    'error': {
                        'code': 'TOKEN_REFRESH_FAILED',
                        'message': 'Failed to refresh token',
                    },
                },
                status_code=401,
                headers=CORS_HEADERS,
            )

        new_access_token = auth_result.get('AccessToken')
        new_id_token = auth_result.get('IdToken')
        expires_in = auth_result.get('ExpiresIn') or 3600
        expires_at = (
            datetime.utcnow() + timedelta(seconds=expires_in)
        ).isoformat() + 'Z'

        logger.info(f"Token refreshed for user {email or 'unknown'}")

        return response(
            {
                'status': 'TOKEN_REFRESHED',
                'data': {
                    'tokens': {
                        'accessToken': {
                            'value': new_access_token,
                            'expiresAt': expires_at,
                        },
                        'idToken': {'value': new_id_token},
                    }
                },
            },
            headers=CORS_HEADERS,
        )

    except Exception as e:
        logger.error(f'Error in refresh_token: {str(e)}', exc_info=True)
        return response(
            {
                'status': 'ERROR',
                'error': {
                    'code': 'TOKEN_REFRESH_FAILED',
                    'message': 'Failed to refresh token',
                },
            },
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
            expires_in = auth_result.get('ExpiresIn') or 3600
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
                        "value": "eyJraWQiOiJ...",
                        "expiresAt": "2026-02-11T08:30:45.123456Z"
                    },
                    "idToken": {
                        "value": "eyJraWQiOiJ..."
                    },
                    "refreshToken": {
                        "value": "eyJjdHkiOiJ..."
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
        "refreshToken": "eyJjdHkiOiJKV1QiLCJlbmMiOiJBMTI4Q0JDLUhTMjU2IiwiYWxnIjoiUlNBLU9BRVAifQ...",
        "email": "user@company.com"
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
                        "value": "eyJraWQiOiJ...",
                        "expiresAt": "2026-02-11T08:30:45.123456Z"
                    },
                    "idToken": {
                        "value": "eyJraWQiOiJ..."
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
                {
                    'status': 'ERROR',
                    'error': {
                        'code': 'INVALID_STAGE',
                        'message': 'Invalid authentication stage',
                    },
                },
                status_code=400,
                headers=CORS_HEADERS,
            )

    except ValueError as e:
        logger.warning(f'Validation error: {str(e)}')
        return response(
            {
                'status': 'ERROR',
                'error': {'code': 'VALIDATION_ERROR', 'message': str(e)},
            },
            status_code=400,
            headers=CORS_HEADERS,
        )

    except Exception as e:
        logger.error(f'Unexpected error: {str(e)}', exc_info=True)
        return response(
            {
                'status': 'ERROR',
                'error': {
                    'code': 'SERVER_ERROR',
                    'message': 'An unexpected error occurred',
                },
            },
            status_code=500,
            headers=CORS_HEADERS,
        )
