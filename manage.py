#!/usr/bin/env python3
"""
Management script for local lambda function execution
"""
import sys
import json
import argparse
from src.functions.city_collector.handler import city_collector
from src.functions.data_scrapper.gmaps_handler import gmaps_scrapper  
from src.functions.data_scrapper.website_handler import website_scrapper
from src.functions.data_scrapper.company_federal_handler import company_federal_scrapper
from src.functions.gl_add_new_lead.add_new_lead_handler import add_new_lead
from src.functions.gl_add_new_company.add_new_company_handler import add_new_company
from src.functions.gl_fetch_leads.fetch_leads_handler import fetch_leads_reminders
from src.functions.gl_fetch_lead_history.fetch_lead_history_handler import fetch_lead_history


# Available lambda functions
LAMBDA_FUNCTIONS = {
    'city_collector': city_collector,
    'gmaps': gmaps_scrapper,
    'website': website_scrapper,
    'company_federal': company_federal_scrapper,
    'add_new_lead': add_new_lead,
    'add_new_company': add_new_company,
    'fetch_leads': fetch_leads_reminders,
    'fetch_lead_history': fetch_lead_history,
}


def execute_lambda(function_name='city_collector', event_file=None, context_file=None):
    """
    Execute the lambda function locally with provided event and context
    
    Args:
        function_name: Name of the lambda function to execute
        event_file: Path to JSON file containing the event data
        context_file: Path to JSON file containing the context data (optional)
    """
    # Get the selected lambda function
    if function_name not in LAMBDA_FUNCTIONS:
        print(f"Error: Unknown function '{function_name}'")
        print(f"Available functions: {', '.join(LAMBDA_FUNCTIONS.keys())}")
        sys.exit(1)
    
    lambda_handler = LAMBDA_FUNCTIONS[function_name]
    
    # Load event data
    if event_file:
        with open(event_file, 'r') as f:
            event = json.load(f)
    else:
        event = {}
    
    # Create a simple context object
    class LambdaContext:
        def __init__(self, context_data=None):
            context_data = context_data or {}
            self.function_name = context_data.get('function_name', 'local-function')
            self.memory_limit_in_mb = context_data.get('memory_limit_in_mb', 128)
            self.invoked_function_arn = context_data.get('invoked_function_arn', 'local-arn')
            self.aws_request_id = context_data.get('aws_request_id', 'local-request-id')
    
    # Load context data if provided
    context_data = {}
    if context_file:
        with open(context_file, 'r') as f:
            context_data = json.load(f)
    
    context = LambdaContext(context_data)
    
    # Execute the lambda handler
    print(f"Executing lambda function: {function_name}")
    print(f"Event: {json.dumps(event, indent=2)}")
    print("-" * 50)
    
    try:
        response = lambda_handler(event, context)
        print("\nResponse:")
        print(json.dumps(response, indent=2))
        return response
    except Exception as e:
        print(f"\nError executing lambda: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Execute lambda function locally')
    parser.add_argument(
        '-f', '--function',
        help='Lambda function to execute',
        choices=list(LAMBDA_FUNCTIONS.keys()),
        default='city_collector'
    )
    parser.add_argument(
        '-e', '--event',
        help='Path to JSON file containing event data',
        default=None
    )
    parser.add_argument(
        '-c', '--context',
        help='Path to JSON file containing context data',
        default=None
    )
    
    args = parser.parse_args()
    execute_lambda(args.function, args.event, args.context)