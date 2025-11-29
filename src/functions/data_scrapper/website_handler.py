"""
Website Scrapper Lambda Handler.

This Lambda function processes SQS messages to scrape and extract
structured company data from websites using LLM-powered extraction.
"""

import json
import os
from http import HTTPStatus
from typing import Any, Dict

from aws_lambda_powertools import Logger

from src.models.scrappers.website_scrapper import WebsiteScrapper
from src.shared.settings import settings

# Configure basic logging
logger = Logger(service='website-scraper-handler')


def website_scrapper(event, context):
    """
    Scrape and extract structured data from company websites.

    This function is triggered by SQS messages containing company_id and website URL.
    It uses WebsiteScrapper to fetch HTML content, extract structured data with LLM,
    and save enriched information to DynamoDB.

    Args:
        event: SQS event containing message with company_id and website
        context: Lambda context object

    Returns:
        Dict with success status and scraping summary
    """
    logger.info('Processing SQS event for website scraping')

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
        website = payload.get('website', '').strip()

        # Validate required fields
        if not company_id or not website:
            error_msg = f'Missing required fields - company_id: {company_id}, website: {website}'
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Get Gemini API key from settings
        try:
            gemini_api_key = settings.gemini_api_key
        except ValueError as e:
            logger.error(f'Configuration error: {str(e)}')
            raise

        logger.info(
            f'Starting website scraper - Stage: {settings.stage}, '
            f'Company: {company_id}, Website: {website}'
        )

        # Instantiate scrapper
        scrapper = WebsiteScrapper(
            company_id=company_id,
            website=website,
            gemini_api_key=gemini_api_key,
        )

        # Execute scraping
        logger.info(f'Scraping website for company {company_id}: {website}')
        scrapper.collect_data()

        # Get final status
        final_status = scrapper.ensamble.get('status')
        status_reason = scrapper.ensamble.get('status_reason')
        pages_fetched = scrapper.ensamble.get('pages_fetched', 0)
        pages_failed = scrapper.ensamble.get('pages_failed', 0)
        data_extracted = scrapper.ensamble.get('data_extracted', {})

        logger.info(
            f'Website scraping completed - Status: {final_status}, '
            f'Reason: {status_reason}, '
            f'Pages fetched: {pages_fetched}, Pages failed: {pages_failed}'
        )

        # Check if scraping failed completely
        if final_status == 'failed' and pages_fetched == 0:
            logger.warning(f'Complete scraping failure for company {company_id}')
            # Still return success to avoid retry - status is saved in DB
            return {
                'statusCode': 200,
                'body': json.dumps(
                    {
                        'success': True,
                        'message': 'Scraping processed but failed - status saved',
                        'company_id': company_id,
                        'website': website,
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
                    'message': 'Website scraping completed successfully',
                    'company_id': company_id,
                    'website': website,
                    'status': final_status,
                    'status_reason': status_reason,
                    'pages_fetched': pages_fetched,
                    'pages_failed': pages_failed,
                    'data_extracted': data_extracted,
                }
            ),
        }

    except Exception as e:
        logger.error(
            f'Error in website_scrapper handler: {str(e)}',
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
