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
            f"Phone number must contain 1-15 digits after normalization. Got: {len(normalized)} digits"
        )
    
    return normalized

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
    result = db_handler.get_item(
        table_name=settings.companies_table_name,
        key={'companyID': company_id}
    )
    
    if not result:
        raise ValueError(f"Company with ID '{company_id}' does not exist")
    

def check_duplicate_phone(company_id: str, phone: str, db_handler: DatabaseHandler) -> None:
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
        # Query the GSI for companyID + phone
        result = db_handler.query(
            table_name=settings.leads_table_name,
            index_name='companyID-phone-index',
            key_condition_expression='companyID = :company_id AND phone = :phone',
            expression_attribute_values={
                ':company_id': company_id,
                ':phone': phone
            }
        )
        
        # Check if any items were returned
        if result and len(result) > 0:
            raise ValueError(
                f"A lead with phone number '{phone}' already exists for this company"
            )
    
    except Exception as e:
        # If GSI doesn't exist yet, provide clear error message
        error_msg = str(e).lower()
        if 'index' in error_msg or 'gsi' in error_msg or 'not found' in error_msg:
            raise ValueError(
                "GSI 'companyID-phone-index' is not available. "
                "Please create the GSI on the leads table before using this endpoint. "
                "See deployment documentation for manual GSI creation steps."
            )
        # Re-raise if it's a duplicate phone error
        if "already exists" in str(e):
            raise
        # Re-raise other unexpected errors
        raise ValueError(f"Error checking for duplicate phone: {str(e)}")