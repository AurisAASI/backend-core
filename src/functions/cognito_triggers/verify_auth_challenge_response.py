"""
Cognito Verify Auth Challenge Response trigger.

This Lambda validates the user's OTP answer against the code stored in DynamoDB.
It is called when the user submits their code via AdminRespondToAuthChallenge.

The validation flow:
1. Extract challengeId from privateChallengeParameters (set in Create trigger)
2. Fetch OTP entry from DynamoDB auth-codes table
3. Compare user's answer with stored code
4. Return answerCorrect = True/False
"""

from typing import Any, Dict

import boto3
from auris_tools.databaseHandlers import DatabaseHandler
from aws_lambda_powertools import Logger

from src.shared.settings import Settings

logger = Logger(service='cognito-verify-auth-challenge')
settings = Settings()

auth_codes_db = DatabaseHandler(table_name=settings.auth_codes_table_name)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Verify auth challenge response handler.

    This trigger validates the OTP code submitted by the user against
    the code stored in DynamoDB.

    Event structure:
    {
        "request": {
            "userAttributes": {...},
            "privateChallengeParameters": {
                "challengeId": "...",
                "email": "..."
            },
            "challengeAnswer": "123456",
            "clientMetadata": {
                "challengeId": "...",
                "email": "..."
            }
        },
        "response": {
            "answerCorrect": true/false
        }
    }

    Args:
        event: Cognito trigger event
        context: Lambda context

    Returns:
        Modified event with answerCorrect set
    """
    request = event.get('request', {})
    private_params = request.get('privateChallengeParameters', {})
    client_metadata = request.get('clientMetadata', {})

    # Get challengeId from either private params or client metadata
    challenge_id = private_params.get('challengeId') or client_metadata.get(
        'challengeId', ''
    )
    email = private_params.get('email') or client_metadata.get('email', '')
    user_answer = request.get('challengeAnswer', '').strip()

    logger.info(f'Verifying challenge for email: {email}, challengeId: {challenge_id}')

    # Default to incorrect
    event['response']['answerCorrect'] = False

    if not challenge_id or not user_answer:
        logger.warning('Missing challengeId or answer')
        return event

    try:
        # Fetch OTP entry from DynamoDB
        stored_data = auth_codes_db._deserialize_item(
            auth_codes_db.get_item(key={'challengeID': challenge_id})
        )

        if not stored_data:
            logger.warning(f'Challenge ID {challenge_id} not found in DynamoDB')
            return event

        stored_code = stored_data.get('code', '')
        stored_email = stored_data.get('email', '')

        # Verify email matches
        if stored_email.lower() != email.lower():
            logger.warning(f'Email mismatch: expected {stored_email}, got {email}')
            return event

        # Check if code has expired
        import time

        current_time = int(time.time())
        expires_at = stored_data.get('expiresAt', 0)

        if current_time > expires_at:
            logger.warning(f'Code expired for challenge ID {challenge_id}')
            return event

        # Compare codes
        if stored_code == user_answer:
            logger.info(f'Code verified successfully for challenge ID {challenge_id}')
            event['response']['answerCorrect'] = True
        else:
            logger.warning(f'Invalid code for challenge ID {challenge_id}')
            # Increment attempts in DynamoDB
            current_attempts = stored_data.get('attempts', 0)
            try:
                auth_codes_db.update_item(
                    key={'challengeID': challenge_id},
                    updates={'attempts': current_attempts + 1},
                    primary_key='challengeID',
                )
            except Exception as e:
                logger.error(f'Failed to update attempts: {str(e)}')

    except Exception as e:
        logger.error(f'Error verifying challenge: {str(e)}', exc_info=True)
        event['response']['answerCorrect'] = False

    return event
