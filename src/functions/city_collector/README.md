# City Collector Lambda Function

## Overview
The City Collector Lambda function is designed to initiate data collection for Brazilian cities by validating city/state combinations and queuing scraping tasks. It integrates with Brasil API for location validation and AWS SQS for task queuing. The function can be triggered by both API Gateway HTTP requests and EventBridge scheduled events.

## Features

- ✅ Multi-source event handling (API Gateway, EventBridge, Direct Invocation)
- ✅ Brazilian city/state validation using Brasil API
- ✅ SQS integration for asynchronous task queuing
- ✅ Comprehensive error handling and logging
- ✅ CORS support for API Gateway
- ✅ 100% test coverage

## Input Format

The function accepts payloads from multiple sources:

### 1. API Gateway Event
```json
{
  "httpMethod": "POST",
  "body": "{\"city_name\":\"São Paulo\",\"state_name\":\"SP\"}",
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
    "city_name": "São Paulo",
    "state_name": "SP"
  }
}
```

### 3. Direct Invocation
```json
{
  "city_name": "São Paulo",
  "state_name": "SP"
}
```

### Required Fields
All event types must include the following fields in their payload:

- `city_name`: The name of the Brazilian city (e.g., "São Paulo", "Rio de Janeiro")
- `state_name`: The Brazilian state abbreviation (e.g., "SP", "RJ", "MG")

## Validation Process

The function performs the following validations:

1. **Payload Validation**: Ensures `city_name` and `state_name` are present
2. **State Validation**: Verifies the state exists in Brazil using Brasil API
3. **City Validation**: Confirms the city exists within the specified state

## Response Format

The function returns different response formats based on the event source:

### API Gateway Response (Success)
```json
{
  "statusCode": 200,
  "headers": {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*"
  },
  "body": "{\"message\":\"City collection initiated\",\"city_name\":\"SÃO PAULO\",\"state_name\":\"SP\"}"
}
```

### EventBridge/Direct Invocation Response (Success)
```json
{
  "success": true,
  "data": {
    "message": "City collection initiated",
    "city_name": "SÃO PAULO",
    "state_name": "SP"
  }
}
```

## Error Handling

The function provides detailed error responses for different scenarios:

### Missing Required Fields (400)
```json
{
  "statusCode": 400,
  "headers": {
    "Content-Type": "application/json"
  },
  "body": "{\"error\":\"Missing required field: city_name\"}"
}
```

### Invalid State (404)
```json
{
  "statusCode": 404,
  "headers": {
    "Content-Type": "application/json"
  },
  "body": "{\"error\":\"State name not found: XX\"}"
}
```

### City Not in State (404)
```json
{
  "statusCode": 404,
  "headers": {
    "Content-Type": "application/json"
  },
  "body": "{\"error\":\"City name not found in the state - City: CURITIBA - State: SP\"}"
}
```

### Internal Server Error (500)
```json
{
  "statusCode": 500,
  "headers": {
    "Content-Type": "application/json"
  },
  "body": "{\"error\":\"Internal Server Error\",\"details\":\"Error description\"}"
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

## AWS Integration

### SQS Queue
The function sends validated city/state combinations to an SQS queue for asynchronous processing by the scraper function.

**Environment Variable**: `SCRAPER_TASK_QUEUE_URL`

**Message Format**:
```json
{
  "city_name": "SÃO PAULO",
  "state_name": "SP"
}
```

### CloudWatch Logging
The function uses AWS Lambda Powertools for structured logging:
- Request/response logging
- Error tracking with stack traces
- SQS message ID tracking
- Brasil API call monitoring

## Testing

A comprehensive test suite is provided in `tests/test_city_collector.py` using **pytest** framework with **100% code coverage**.

### Test Coverage

The test suite includes:
- ✅ **Extract Payload Tests** (4 tests): API Gateway, EventBridge, Direct Invocation
- ✅ **Validate Payload Tests** (4 tests): Valid/invalid scenarios
- ✅ **City Collector Tests** (11 tests): Success cases, error handling, validation
- ✅ **Integration Tests** (2 tests): SQS messaging, Brasil API integration

**Total: 19 tests**

### Running Tests

Run all tests:
```bash
pytest tests/test_city_collector.py -v
```

Run with coverage report:
```bash
pytest tests/test_city_collector.py --cov=src/functions/city_collector --cov-report=term-missing
```

Run specific test class:
```bash
pytest tests/test_city_collector.py::TestCityCollector -v
```

### Test Fixtures

The test suite uses pytest fixtures for:
- `lambda_context`: Mock Lambda context
- `valid_payload`: Sample Brazilian city/state data
- `mock_brasil_api_success`: Mocked Brasil API responses
- `mock_sqs_client`: Mocked AWS SQS client
- `mock_env_vars`: Environment variable configuration

## Implementation Details

### Helper Functions

#### `extract_payload(event: Dict[str, Any]) -> Dict[str, Any]`
Extracts payload from different event sources (API Gateway, EventBridge, or direct invocation).

#### `validate_payload(payload: Dict[str, Any]) -> Optional[Dict[str, str]]`
Validates that required fields (`city_name`, `state_name`) are present in the payload.

### Main Handler Flow

1. **Extract Payload**: Determine event source and extract payload data
2. **Validate Payload**: Check for required fields
3. **Validate State**: Call Brasil API to verify state exists
4. **Validate City**: Call Brasil API to verify city exists in the state
5. **Queue Task**: Send message to SQS queue for scraper processing
6. **Return Response**: Format response based on event source (API Gateway vs EventBridge)

## External Dependencies

- **Brasil API**: Used for Brazilian geographic data validation
  - State validation: `https://brasilapi.com.br/api/ibge/uf/v1/{state}`
  - City validation: `https://brasilapi.com.br/api/ibge/municipios/v1/{state}`
- **AWS SQS**: Queue for asynchronous task processing
- **AWS Lambda Powertools**: Structured logging and monitoring

## Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `SCRAPER_TASK_QUEUE_URL` | SQS queue URL for scraper tasks | Yes |
| `STAGE` | Deployment stage (dev/test/prod) | No |
| `FUNCTION_NAME` | Lambda function name | No |

## Error Codes

| Status Code | Description |
|-------------|-------------|
| 200 | Success - City collection initiated |
| 400 | Bad Request - Missing required fields |
| 404 | Not Found - Invalid state or city not in state |
| 500 | Internal Server Error - Unexpected error occurred |