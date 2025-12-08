import json
import re
from http import HTTPStatus
from typing import Any, Dict, Optional, Union


def validate_cnpj(cnpj: str) -> bool:
    """
    Validate Brazilian CNPJ number format and check digit.

    Args:
        cnpj: CNPJ string in format XX.XXX.XXX/XXXX-XX or digits only

    Returns:
        True if valid, False otherwise
    """
    if not cnpj:
        return False

    # Remove formatting characters
    cnpj_digits = re.sub(r'[^\d]', '', cnpj)

    # Check if has 14 digits
    if len(cnpj_digits) != 14:
        return False

    # Check if all digits are the same (invalid CNPJ)
    if cnpj_digits == cnpj_digits[0] * 14:
        return False

    # Validate check digits
    def calculate_check_digit(cnpj_partial: str, weights: list) -> int:
        """Calculate CNPJ check digit."""
        total = sum(int(digit) * weight for digit, weight in zip(cnpj_partial, weights))
        remainder = total % 11
        return 0 if remainder < 2 else 11 - remainder

    # First check digit
    weights_first = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    first_check = calculate_check_digit(cnpj_digits[:12], weights_first)

    if int(cnpj_digits[12]) != first_check:
        return False

    # Second check digit
    weights_second = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    second_check = calculate_check_digit(cnpj_digits[:13], weights_second)

    return int(cnpj_digits[13]) == second_check


def clean_cnpj(cnpj: str) -> Optional[str]:
    """
    Clean and format CNPJ to digits only.

    Args:
        cnpj: CNPJ string with or without formatting

    Returns:
        14-digit CNPJ string or None if invalid
    """
    if not cnpj:
        return None

    # Remove all non-digit characters
    cnpj_digits = re.sub(r'[^\d]', '', cnpj)

    # Validate length
    if len(cnpj_digits) != 14:
        return None

    return cnpj_digits


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
