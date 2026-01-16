import json
import re
from http import HTTPStatus
from typing import Any, Dict, Optional, Union

from auris_tools.databaseHandlers import DatabaseHandler

from src.shared.settings import settings


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


def normalize_phone(phone: str) -> str:
    """
    Normalize phone number to digits only and validate length.

    Args:
        phone: Phone number string (may contain formatting)

    Returns:
        Normalized phone string (digits only)

    Raises:
        ValueError: If phone length is not between 1-15 digits
    """
    # Strip all non-digit characters
    normalized = re.sub(r'\D', '', phone)

    # Validate length
    if not normalized or len(normalized) < 1 or len(normalized) > 15:
        raise ValueError(
            f'Phone number must contain 1-15 digits after normalization. Got: {len(normalized)} digits'
        )

    return normalized


def normalize_string(text: str) -> str:
    """
    Normalize string for comparison by converting to lowercase and stripping whitespace.

    Args:
        text: String to normalize

    Returns:
        Normalized string (lowercase, stripped)
    """
    if not text:
        return ''

    # Convert to lowercase and strip leading/trailing whitespace
    # Also collapse multiple spaces into single space
    return ' '.join(text.lower().strip().split())


def calculate_levenshtein_distance(s1: str, s2: str) -> int:
    """
    Calculate Levenshtein distance between two strings using dynamic programming.

    The Levenshtein distance is the minimum number of single-character edits
    (insertions, deletions, or substitutions) required to change one string into another.

    Args:
        s1: First string
        s2: Second string

    Returns:
        Integer distance between the strings
    """
    # Handle empty strings
    if not s1:
        return len(s2)
    if not s2:
        return len(s1)

    # Create matrix with dimensions (len(s1)+1) x (len(s2)+1)
    rows = len(s1) + 1
    cols = len(s2) + 1
    matrix = [[0 for _ in range(cols)] for _ in range(rows)]

    # Initialize first column (transform s1 to empty string)
    for i in range(rows):
        matrix[i][0] = i

    # Initialize first row (transform empty string to s2)
    for j in range(cols):
        matrix[0][j] = j

    # Fill matrix using dynamic programming
    for i in range(1, rows):
        for j in range(1, cols):
            # Cost of substitution (0 if characters match, 1 if different)
            cost = 0 if s1[i - 1] == s2[j - 1] else 1

            matrix[i][j] = min(
                matrix[i - 1][j] + 1,  # Deletion
                matrix[i][j - 1] + 1,  # Insertion
                matrix[i - 1][j - 1] + cost,  # Substitution
            )

    return matrix[rows - 1][cols - 1]


def calculate_similarity_ratio(s1: str, s2: str, normalize: bool = True) -> float:
    """
    Calculate similarity ratio between two strings using Levenshtein distance.

    Returns a percentage (0-100) where 100 means identical strings and 0 means
    completely different. Automatically normalizes strings before comparison.

    Args:
        s1: First string
        s2: Second string
        normalize: Whether to normalize strings before comparison (default: True)

    Returns:
        Similarity ratio as percentage (0-100)
    """
    # Normalize strings if requested
    if normalize:
        s1 = normalize_string(s1)
        s2 = normalize_string(s2)

    # Handle empty strings
    if not s1 and not s2:
        return 100.0
    if not s1 or not s2:
        return 0.0

    # Calculate Levenshtein distance
    distance = calculate_levenshtein_distance(s1, s2)

    # Calculate similarity ratio based on longest string
    max_len = max(len(s1), len(s2))
    similarity = ((max_len - distance) / max_len) * 100

    return round(similarity, 2)


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


def validate_company_exists(company_id: str, db_handler: DatabaseHandler) -> None:
    """
    Validate that the company exists in the companies table.

    Args:
        company_id: Company ID to validate
        db_handler: DatabaseHandler instance for companies table

    Raises:
        ValueError: If company does not exist
    """
    result = db_handler.get_item(key={'companyID': company_id})

    if not result:
        raise ValueError(f"Company with ID '{company_id}' does not exist")


def check_duplicate_phone(
    company_id: str, phone: str, db_handler: DatabaseHandler
) -> None:
    """
    Check for duplicate phone number within the same company using GSI.

    Args:
        company_id: Company ID to scope the duplicate check
        phone: Normalized phone number
        db_handler: DatabaseHandler instance for leads table

    Raises:
        ValueError: If duplicate phone found or GSI is not available
    """
    try:
        # Query the GSI using boto3 client (DatabaseHandler doesn't have a query method)
        # boto3 client uses PascalCase parameter names
        response = db_handler.client.query(
            TableName=settings.leads_table_name,
            IndexName='companyID-phone-index',
            KeyConditionExpression='companyID = :company_id AND phone = :phone',
            ExpressionAttributeValues={
                ':company_id': {'S': company_id},
                ':phone': {'S': phone},
            },
        )

        # Check if any items were returned
        items = response.get('Items', [])
        if items and len(items) > 0:
            raise ValueError(
                f"A lead with phone number '{phone}' already exists for this company"
            )

    except Exception as e:
        # If GSI doesn't exist yet, provide clear error message
        error_msg = str(e).lower()
        if 'index' in error_msg or 'gsi' in error_msg or 'not found' in error_msg:
            raise ValueError(
                "GSI 'companyID-phone-index' is not available. "
                'Please create the GSI on the leads table before using this endpoint. '
                'See deployment documentation for manual GSI creation steps.'
            )
        # Re-raise if it's a duplicate phone error
        if 'already exists' in str(e):
            raise
        # Re-raise other unexpected errors
        raise ValueError(f'Error checking for duplicate phone: {str(e)}')
