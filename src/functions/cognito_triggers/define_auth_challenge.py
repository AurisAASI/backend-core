"""
Cognito Define Auth Challenge trigger.

This Lambda defines the authentication flow for custom auth challenges.
It determines whether to:
- Issue a CUSTOM_CHALLENGE (for OTP verification)
- Issue tokens (authentication succeeded)
- Fail authentication (max attempts exceeded or invalid state)

Flow:
1. First request (no session) → CUSTOM_CHALLENGE
2. After correct answer → Issue tokens
3. After incorrect answer → CUSTOM_CHALLENGE (retry) or fail if max attempts
"""

from typing import Any, Dict

from aws_lambda_powertools import Logger

logger = Logger(service='cognito-define-auth-challenge')


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Define auth challenge handler.

    Event structure:
    {
        "request": {
            "userAttributes": {...},
            "session": [
                {
                    "challengeName": "CUSTOM_CHALLENGE",
                    "challengeResult": true/false
                }
            ],
            "clientMetadata": {
                "challengeId": "...",
                "email": "..."
            }
        },
        "response": {
            "challengeName": "CUSTOM_CHALLENGE" | null,
            "issueTokens": true/false,
            "failAuthentication": true/false
        }
    }

    Args:
        event: Cognito trigger event
        context: Lambda context

    Returns:
        Modified event with response fields set
    """
    request = event.get('request', {})
    session = request.get('session', [])

    logger.info(f'Define auth challenge - Session length: {len(session)}')

    # First request (no previous session) → issue CUSTOM_CHALLENGE
    if len(session) == 0:
        logger.info('First auth attempt - issuing CUSTOM_CHALLENGE')
        event['response']['challengeName'] = 'CUSTOM_CHALLENGE'
        event['response']['issueTokens'] = False
        event['response']['failAuthentication'] = False
        return event

    # Check if last challenge was answered correctly
    last_challenge = session[-1]
    challenge_result = last_challenge.get('challengeResult', False)

    if challenge_result:
        # Correct answer → issue tokens
        logger.info('Challenge answered correctly - issuing tokens')
        event['response']['challengeName'] = None
        event['response']['issueTokens'] = True
        event['response']['failAuthentication'] = False
    else:
        # Incorrect answer → check max attempts (Cognito default is 3)
        max_attempts = 3
        attempt_count = len(session)

        if attempt_count >= max_attempts:
            logger.warning(
                f'Max attempts ({max_attempts}) exceeded - failing authentication'
            )
            event['response']['challengeName'] = None
            event['response']['issueTokens'] = False
            event['response']['failAuthentication'] = True
        else:
            logger.info(
                f'Challenge failed, attempt {attempt_count}/{max_attempts} - retrying'
            )
            event['response']['challengeName'] = 'CUSTOM_CHALLENGE'
            event['response']['issueTokens'] = False
            event['response']['failAuthentication'] = False

    return event
