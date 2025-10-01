# City Collector Lambda Function

## Overview
The City Collector Lambda function is designed to collect city-specific data by integrating with other AWS services. It can be triggered by both API Gateway HTTP requests and EventBridge scheduled events.

## Input Format

The function accepts payloads from two different sources:

### 1. API Gateway Event
```json
{
  "httpMethod": "POST",
  "body": "{\"city_name\":\"Austin\",\"state_name\":\"Texas\"}",
  "headers": {
    "Content-Type": "application/json"
  }
}
```

### 2. EventBridge Event
```json
{
  "source": "aws.events",
  "detail-type": "Scheduled Event",
  "detail": {
    "city_name": "Austin",
    "state_name": "Texas"
  }
}
```

### Required Fields
Both event types must include the following fields in their payload:

- `city_name`: The name of the city to collect data for
- `state_name`: The name of the state containing the city

## Response Format

The function returns different response formats based on the event source:

### API Gateway Response
```json
{
  "statusCode": 200,
  "headers": {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*"
  },
  "body": "{\"message\":\"City collection initiated\",\"stage\":\"dev\",\"function_name\":\"city-collector\",\"city_name\":\"Austin\",\"state_name\":\"Texas\"}"
}
```

### EventBridge/Direct Invocation Response
```json
{
  "success": true,
  "data": {
    "message": "City collection initiated",
    "stage": "dev",
    "function_name": "city-collector",
    "city_name": "Austin",
    "state_name": "Texas"
  }
}
```

## Error Handling

If required fields are missing or if any error occurs during processing, appropriate error responses are returned:

### API Gateway Error Response
```json
{
  "statusCode": 400,
  "headers": {
    "Content-Type": "application/json"
  },
  "body": "{\"error\":\"Missing required field: city_name\"}"
}
```

### EventBridge/Direct Invocation Error Response
```json
{
  "success": false,
  "error": {
    "error": "Internal Server Error",
    "details": "Error message"
  }
}
```

## Testing

A test suite is provided in `tests/test_city_collector.py` to validate the function's behavior with different event types and edge cases.

To run the tests:
```bash
python -m unittest tests/test_city_collector.py
```

## Implementation Details

The function processes events through these main steps:

1. Extract payload from different event sources
2. Validate that required fields are present
3. Process the city and state data
4. Return an appropriate response based on the event source