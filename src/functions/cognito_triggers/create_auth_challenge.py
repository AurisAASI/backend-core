"""
Cognito Create Auth Challenge trigger.

This Lambda is called when Cognito needs to create a custom authentication challenge.
Since we generate and send the OTP code in our login_handler (handle_send_code),
this trigger simply returns metadata/placeholders without generating a new code.

The actual OTP generation and email sending happens in the login lambda before
AdminInitiateAuth is called.
"""

from typing import Any, Dict

from aws_lambda_powertools import Logger

logger = Logger(service='cognito-create-auth-challenge')


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Create auth challenge handler.

    This trigger is invoked when Cognito needs to create a CUSTOM_CHALLENGE.
    Since we handle OTP generation and email in the login lambda (handle_send_code),
    we don't generate anything here - just return metadata.

    Event structure:
    {
        "request": {
            "userAttributes": {...},
            "challengeName": "CUSTOM_CHALLENGE",
            "session": [...],
            "clientMetadata": {
                "challengeId": "...",
                "email": "..."
            }
        },
        "response": {
            "publicChallengeParameters": {},
            "privateChallengeParameters": {},
            "challengeMetadata": "..."
        }
    }

    Args:
        event: Cognito trigger event
        context: Lambda context

    Returns:
        Modified event with challenge parameters
    """
    request = event.get('request', {})
    client_metadata = request.get('clientMetadata', {})
    challenge_id = client_metadata.get('challengeId', '')
    email = client_metadata.get('email', '')

    logger.info(
        f'Creating auth challenge for email: {email}, challengeId: {challenge_id}'
    )

    # Public parameters visible to client (can be empty since client already has challengeId)
    event['response']['publicChallengeParameters'] = {
        'email': email,
    }

    # Private parameters used for verification (stored by Cognito, passed to Verify trigger)
    event['response']['privateChallengeParameters'] = {
        'challengeId': challenge_id,
        'email': email,
    }

    # Metadata for logging/tracking
    event['response']['challengeMetadata'] = f'EMAIL_OTP_{challenge_id}'

    logger.info(f'Challenge created with metadata: EMAIL_OTP_{challenge_id}')

    return event
