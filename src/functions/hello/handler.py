import json
import os
import logging


# Configure basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(event, context):
    """
    Simple Lambda handler function that returns a greeting message
    
    Parameters:
    - event: The event data from the API Gateway or other trigger
    - context: Lambda runtime information
    
    Returns:
    - API Gateway response object with statusCode, headers, and body
    """
    logger.info(f"Processing event: {event}")
    
    try:
        # Get environment variables or use defaults
        stage = os.environ.get("STAGE", "local")
        function_name = os.environ.get("FUNCTION_NAME", "hello-function")
        
        # Create response body
        response_body = {
            "message": "Hello from backend-core!",
            "stage": stage,
            "function_name": function_name
        }
        
        # Return formatted API Gateway response
        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"  # Enable CORS
            },
            "body": json.dumps(response_body)
        }
        
    except Exception as e:
        logger.error(f"Error processing request: {str(e)}")
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": "Internal Server Error"})
        }
