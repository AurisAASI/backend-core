"""
Company Federal Scrapper Lambda Handler.

This Lambda function processes SQS messages to fetch federal company data
from OpenCNPJ API using CNPJ numbers extracted from websites.
"""

import json
from http import HTTPStatus

from aws_lambda_powertools import Logger

from src.models.scrappers.company_federal_scrapper import CompanyFederalScrapper
from src.shared.settings import settings
from src.shared.utils import clean_cnpj, validate_cnpj

# Configure basic logging
logger = Logger(service='company-federal-handler')


def company_federal_scrapper(event, context):
    """
    Fetch and save federal company data from OpenCNPJ API.

    This function is triggered by SQS messages containing company_id and CNPJ.
    It validates the CNPJ, fetches official company data from OpenCNPJ API,
    and saves enriched information to DynamoDB.

    Args:
        event: SQS event containing message with company_id and cnpj
        context: Lambda context object

    Returns:
        Dict with success status and scraping summary
    """
    logger.info('Processing SQS event for federal company data collection')

    try:
        # Extract the single SQS record
        records = event.get('Records', [])
        if not records:
            logger.error('No SQS records found in event')
            raise ValueError('No SQS records in event')

        record = records[0]
        message_id = record.get('messageId')

        # Extract message body
        body = record.get('body', '{}')
        if isinstance(body, str):
            payload = json.loads(body)
        else:
            payload = body

        logger.info(f'Processing message {message_id}: {payload}')

        # Extract required fields
        company_id = payload.get('company_id', '').strip()
        cnpj = payload.get('cnpj', '').strip()

        # Validate required fields
        if not company_id or not cnpj:
            error_msg = (
                f'Missing required fields - company_id: {company_id}, cnpj: {cnpj}'
            )
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Clean and validate CNPJ
        cleaned_cnpj = clean_cnpj(cnpj)
        if not cleaned_cnpj:
            error_msg = f'Invalid CNPJ format: {cnpj}'
            logger.error(error_msg)
            raise ValueError(error_msg)

        if not validate_cnpj(cleaned_cnpj):
            error_msg = f'CNPJ failed validation check: {cnpj}'
            logger.error(error_msg)
            raise ValueError(error_msg)

        logger.info(
            f'Starting federal data scraper - Stage: {settings.stage}, '
            f'Company: {company_id}, CNPJ: {cleaned_cnpj}'
        )

        # Instantiate scrapper
        scrapper = CompanyFederalScrapper(
            company_id=company_id,
            cnpj=cleaned_cnpj,
        )

        # Execute scraping
        logger.info(f'Fetching federal data for company {company_id}, CNPJ: {cleaned_cnpj}')
        scrapper.collect_data()

        # Get final status
        final_status = scrapper.ensamble.get('status')
        status_reason = scrapper.ensamble.get('status_reason')
        data_fetched = scrapper.ensamble.get('data_fetched', False)

        logger.info(
            f'Federal data collection completed - Status: {final_status}, '
            f'Reason: {status_reason}, Data fetched: {data_fetched}'
        )

        # Check if scraping failed completely
        if final_status == 'failed' and not data_fetched:
            logger.warning(f'Complete federal scraping failure for company {company_id}')
            # Still return success to avoid retry - status is saved in DB
            return {
                'statusCode': 200,
                'body': json.dumps(
                    {
                        'success': True,
                        'message': 'Federal scraping processed but failed - status saved',
                        'company_id': company_id,
                        'cnpj': cleaned_cnpj,
                        'status': final_status,
                        'status_reason': status_reason,
                    }
                ),
            }

        # Return success response
        return {
            'statusCode': 200,
            'body': json.dumps(
                {
                    'success': True,
                    'message': 'Federal data collection completed successfully',
                    'company_id': company_id,
                    'cnpj': cleaned_cnpj,
                    'status': final_status,
                    'status_reason': status_reason,
                    'data_fetched': data_fetched,
                }
            ),
        }

    except Exception as e:
        logger.error(
            f'Error in company_federal_scrapper handler: {str(e)}',
            exc_info=True,
        )
        return {
            'statusCode': 500,
            'body': json.dumps(
                {
                    'success': False,
                    'error': 'Internal Server Error',
                    'details': str(e),
                }
            ),
        }
