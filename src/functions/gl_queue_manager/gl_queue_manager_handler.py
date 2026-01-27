import json
import os
from http import HTTPStatus
from typing import Any, Dict, Optional

import boto3
from aws_lambda_powertools import Logger

from src.shared.settings import settings
from src.shared.utils import response

# Configure basic logging
logger = Logger(service='gl-queue-manager')


# Mapping of operationType to target Lambda function names
# This will be expanded based on actual requirements
OPERATION_TYPE_MAPPING: Dict[str, str] = {
    'communication_registration': 'gl_communication_registration',  # Lambda function name
}


def gl_queue_manager(event, context):
    """
    Hub lambda function that routes operations to appropriate lambda services based on operationType.

    This function is triggered by SQS messages from the operations queue.
    It validates the operationType, enriches the message with userEmail and companyID,
    and invokes the appropriate target Lambda function synchronously.

    SQS Event Structure:
    {
        "Records": [
            {
                "messageId": "...",
                "body": "{\"operationType\": \"communication_registration\", \"payload\": {...}, ...}"  // JSON STRING
            }
        ]
    }

    Message Body Structure (after JSON.parse of event.Records[0].body):
    {
        "operationType": "communication_registration",  // Required: operation type to route
        "payload": { ... },                              // Required: operation-specific data
        "userEmail": "user@example.com",                 // Required: user who initiated
        "companyID": "company-123"                       // Required: associated company
        "timestamp": "2026-01-21T10:00:00.000Z",        // Optional: operation timestamp
    }

    Args:
        event: SQS event containing message(s) with operation details
        context: Lambda context object

    Returns:
        Dict with success status (200) or error status
    """

    # TODO: RETIRADO O SERVIÇO COMMUNCATION_REGISTRATION PARA SER DIRETO COM API GATEWAY. DEIXEI ESSA FILA AQUI PARA PODER INCLUIR OUTROS SERVIÇOS DEPOIS (REPORTS, ETC)
    logger.info(f'Processing SQS event: {json.dumps(event)}')

    try:
        # Extract the single SQS record (batchSize: 1)
        records = event.get('Records', [])
        if not records:
            error_msg = 'No SQS records found in event'
            logger.error(error_msg)
            return response(
                status_code=HTTPStatus.BAD_REQUEST, message={'error': error_msg}
            )

        record = records[0]
        message_id = record.get('messageId')

        # Extract message body
        body = record.get('body', '{}')
        if isinstance(body, str):
            try:
                payload = json.loads(body)
            except json.JSONDecodeError as e:
                error_msg = f'Invalid JSON in message body: {str(e)}'
                logger.error(error_msg)
                return response(
                    status_code=HTTPStatus.BAD_REQUEST, message={'error': error_msg}
                )
        else:
            payload = body

        logger.info(f'Processing message {message_id}: {json.dumps(payload)}')

        # Extract and validate required fields
        operation_type = payload.get('operationType', '').strip().lower()
        operation_payload = payload.get('payload')
        user_email = payload.get('userEmail', '').strip()
        company_id = payload.get('companyID', '').strip()
        timestamp = payload.get('timestamp', '')

        # Validate operationType and payload
        try:
            _validate_operation_type(operation_type)
            _validate_payload_by_operation_type(operation_type, operation_payload)
        except ValueError as e:
            return response(
                status_code=HTTPStatus.BAD_REQUEST, message={'error': str(e)}
            )

        # Get target Lambda function name
        target_function_name = OPERATION_TYPE_MAPPING[operation_type]

        # Prepare Lambda invocation payload with enriched metadata
        region = os.environ.get('AWS_REGION_NAME', settings.region)
        lambda_client = boto3.client('lambda', region_name=region)

        # Construct enriched payload for target Lambda
        enriched_payload = {
            'payload': operation_payload,
            'metadata': {
                'userEmail': user_email,
                'companyID': company_id,
                'operationType': operation_type,
                'timestamp': timestamp,
            },
        }

        logger.info(
            f'Invoking Lambda function {target_function_name} for operation {operation_type}'
        )

        # Invoke target Lambda function synchronously
        try:
            lambda_response = lambda_client.invoke(
                FunctionName=target_function_name,
                InvocationType='RequestResponse',  # Synchronous invocation
                Payload=json.dumps(enriched_payload),
            )

            # Read and parse Lambda response
            response_payload = json.loads(lambda_response['Payload'].read())
            status_code = lambda_response['StatusCode']
        except Exception as e:
            error_msg = f'Error invoking Lambda {target_function_name}: {str(e)}'
            logger.error(error_msg)
            return response(
                status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
                message={'error': error_msg},
            )

        logger.info(
            f'Successfully invoked Lambda {target_function_name}. '
            f'Status: {status_code}, Response: {response_payload}'
        )

        # Return Lambda invocation result
        return response(
            status_code=HTTPStatus.OK,
            message={
                'message': 'Operation executed successfully',
                'operationType': operation_type,
                'lambdaStatusCode': status_code,
                'result': response_payload,
            },
        )

    except Exception as e:
        error_msg = f'Unexpected error processing operation: {str(e)}'
        logger.exception(error_msg)
        return response(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR, message={'error': error_msg}
        )


def _validate_operation_type(operation_type: str) -> None:
    """Validate the operationType field.

    Args:
        operation_type (str): The operation type to validate.

    Raises:
        ValueError: If operationType is missing or invalid.
    """
    if not operation_type:
        error_msg = 'Missing required field: operationType'
        logger.error(error_msg)
        raise ValueError(error_msg)

    if operation_type not in OPERATION_TYPE_MAPPING:
        error_msg = (
            f'Invalid operationType: {operation_type}. '
            f'Supported types: {list(OPERATION_TYPE_MAPPING.keys())}'
        )
        logger.error(error_msg)
        raise ValueError(error_msg)


def _validate_payload_by_operation_type(operation_type: str, payload: Any) -> None:
    """Validate the payload field based on operationType.

    Args:
        operation_type (str): The operation type.
        payload (Any): The payload to validate.

    Raises:
        ValueError: If payload is missing or invalid for the operation type.
    """
    if payload is None:
        error_msg = 'Missing required field: payload'
        logger.error(error_msg)
        raise ValueError(error_msg)

    # # Add specific payload validations per operationType as needed
    # if operation_type == 'communication_registration':
    #     if not isinstance(payload, dict):
    #         error_msg = (
    #             'Invalid payload for communication_registration: must be a JSON object'
    #         )
    #         logger.error(error_msg)
    #         raise ValueError(error_msg)
    #     # Further field validations can be added here
