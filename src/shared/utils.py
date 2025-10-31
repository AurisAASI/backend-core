import json
from http import HTTPStatus
from typing import Any, Dict, Optional, Union


def response(
    message: str,
    status_code: Union[int, HTTPStatus] = HTTPStatus.OK,
    headers: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Build a standardized API Gateway-style response dictionary for AWS Lambda handlers.

    Args:
        message: Content to be serialized as JSON for the response body.
        status_code: HTTP status code (int or HTTPStatus). Defaults to HTTPStatus.OK.
        headers: Additional headers to include in the response. These are merged into
        the default headers.

    Returns:
        Dict[str, Any]: Dictionary ready to return from the Lambda handler with keys:
            - statusCode (int): Numeric HTTP status code.
            - headers (Dict[str, str]): Response headers (defaults to {"Content-Type": "application/json"} merged with any provided headers).
            - body (str): JSON-serialized representation of `message`.
    """
    sc = int(status_code)
    default_headers = {'Content-Type': 'application/json'}
    if headers:
        default_headers.update(headers)

    return {
        'statusCode': sc,
        'headers': default_headers,
        'body': json.dumps(message),
    }
