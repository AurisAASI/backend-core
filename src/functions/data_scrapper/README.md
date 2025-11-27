# Data Scrapper Lambda Function

## Overview
The Data Scrapper Lambda function processes scraping tasks from SQS queue to collect comprehensive public information about AASI companies (hearing aid providers) using Google Places API. It performs exhaustive searches with multiple search terms, enriches data with detailed information, handles deduplication, and persists results to DynamoDB with quota management and status tracking.

## Features

- ✅ Google Places API Text Search with pagination
- ✅ Google Places API Details for data enrichment
- ✅ Quota tracking and management (20,000 units/day default)
- ✅ Geolocation-based duplicate detection (50-meter threshold)
- ✅ DynamoDB persistence with company-place linking
- ✅ Comprehensive error handling and logging
- ✅ Partial result saving on quota exceeded
- ✅ Collection status tracking

## Input Format

The function is triggered by SQS messages from the `city_collector` function:

### SQS Message Body
```json
{
  "city": "SÃO PAULO",
  "state": "SP",
  "niche": "aasi"
}
```

### Required Fields
- `city`: City name (string, uppercase)
- `state`: State abbreviation (string, uppercase)
- `niche`: Business niche (string, options: "aasi", "orl", "geria", "audiologist")

## Processing Flow

### 1. Text Search Phase
For each search term in `niche_terms.json`:
- Constructs query: `"{term} in {city}, {state}, Brazil"`
- Calls Google Places Text Search API
- Handles pagination with `next_page_token`
- Collects: `place_id`, `name`, `formatted_address`, `geometry`, `rating`, `user_ratings_total`, `business_status`, `types`, `photos`, `opening_hours`
- Costs: **32 quota units per request**

### 2. Details Enrichment Phase
For each unique `place_id`:
- Calls Google Places Details API
- Enriches with: `formatted_phone_number`, `international_phone_number`, `website`, `url`, `opening_hours` (detailed), `reviews`, `price_level`
- Costs: **17 quota units per request**

### 3. Deduplication
- **By place_id**: Skip if same Google Place ID already processed
- **By location**: Skip if within 50 meters of existing place (uses Haversine formula)

### 4. Database Persistence
- Checks if place exists in `dev-auris-core-places` table
- For existing places: updates if data changed, skips if identical
- For new places:
  - Generates UUID as `companyID`
  - Inserts record in `dev-auris-core-companies` table
  - Inserts record in `dev-auris-core-places` table with `companyID` link

## Quota Management

### Daily Quota Limit
- Default: **20,000 units** (configurable via `GOOGLE_PLACES_DAILY_QUOTA_LIMIT`)
- Text Search: **32 units** per request
- Place Details: **17 units** per place

### Quota Thresholds
- **80%**: Info log warning
- **90%**: Warning log alert
- **100%**: Collection stops, partial results saved

### Example Calculation
For 3 search terms × 20 results × 2 pages = 120 places:
- Text Search: `3 terms × 2 pages × 32 units = 192 units`
- Place Details: `120 places × 17 units = 2,040 units`
- **Total: ~2,232 quota units per city**

## DynamoDB Schema

### dev-auris-core-companies Table
```json
{
  "companyID": "uuid-v4",           // Primary Key
  "name": "Company Name",
  "city": "CITY",
  "state": "ST",
  "niche": "aasi",
  "collection_status": "completed",
  "collection_reason": "Collection completed successfully"
}
```

### dev-auris-core-places Table
```json
{
  "placeID": "google-place-id",     // Primary Key
  "companyID": "uuid-v4",           // Foreign Key to companies table
  "place_id": "google-place-id",
  "name": "Place Name",
  "formatted_address": "Full Address",
  "geometry": {
    "location": {
      "lat": -23.5505199,
      "lng": -46.6333094
    }
  },
  "rating": 4.5,
  "user_ratings_total": 120,
  "formatted_phone_number": "(11) 1234-5678",
  "international_phone_number": "+55 11 1234-5678",
  "website": "https://example.com",
  "url": "https://maps.google.com/?cid=...",
  "business_status": "OPERATIONAL",
  "types": ["store", "health"],
  "opening_hours": { ... },
  "reviews": [ ... ],
  "price_level": 2,
  "photos": [ ... ]
}
```

## Collection Status

### Status Values
- `in_progress`: Collection is ongoing
- `completed`: Collection finished successfully
- `completed_no_results`: No places found
- `partial_quota_exceeded`: Stopped due to quota limit, partial results saved
- `failed_no_search_terms`: No search terms configured for niche
- `failed_api_error`: Google Places API error
- `failed_database_error`: DynamoDB operation error

### Status Reason
Human-readable explanation stored in `status_reason` field:
- `"Collection completed successfully"`
- `"API quota limit reached at 18500 units out of 20000"`
- `"Text search API request failed: Connection timeout"`
- `"Database save failed: AccessDeniedException"`

## Environment Variables

| Variable | Description | Required | Default |
|----------|-------------|----------|---------|
| `GOOGLE_PLACES_API_KEY` | Google Places API key | Yes | - |
| `GOOGLE_PLACES_DAILY_QUOTA_LIMIT` | Daily quota limit | No | 20000 |
| `FUNCTION_NAME` | Lambda function name | No | - |

## Search Terms Configuration

Search terms are defined in `src/models/scrappers/niche_terms.json`:

```json
{
  "aasi": [
    "aparelhos auditivos",
    "loja aparelhos auditivos",
    "centros auditivos"
  ],
  "orl": [],
  "geria": [],
  "audiologist": []
}
```

## Response Format

### Success Response (SQS Batch)
```json
{
  "batchItemFailures": []
}
```

### Partial Failure Response
```json
{
  "batchItemFailures": [
    {"itemIdentifier": "message-id-1"},
    {"itemIdentifier": "message-id-2"}
  ]
}
```

Failed messages are automatically retried by SQS (up to 3 times before moving to DLQ).

## Logging

Uses AWS Lambda Powertools for structured logging:

### Log Levels
- **INFO**: Normal operations (search progress, database operations, summary)
- **WARNING**: Quota thresholds, duplicate detection, non-critical errors
- **ERROR**: API failures, database errors, validation failures
- **DEBUG**: Detailed place information, API responses

### Sample Log Entries
```
INFO: Starting place collection for SÃO PAULO, SP - Niche: aasi, Terms: 3
INFO: Processing search term 1/3: "aparelhos auditivos"
INFO: Text search page 1: found 20 places (total: 20)
INFO: Fetched details for place: Audicare (ID: ChIJ...)
INFO: Duplicate location found: Centro Auditivo - Distance: 35.42m
WARNING: API quota at 80%: 16000/20000
INFO: Database save completed - New: 45, Updated: 12, Skipped: 8
INFO: Final status: completed - Collection completed successfully
```

## Error Handling

### API Errors
- Connection timeouts: Retry with exponential backoff (handled by SQS)
- Rate limiting: Quota management prevents hitting limits
- Invalid API key: Fails message for retry
- Individual place errors: Logged and skipped, processing continues

### Database Errors
- Connection issues: Fails entire batch for retry
- Permission errors: Fails with status `failed_database_error`
- Individual record errors: Logged and skipped

### Quota Exceeded
- Stops collection gracefully
- Saves partial results to database
- Sets status to `partial_quota_exceeded`
- Message marked as successful (not retried)

## Performance Considerations

### Timing
- **Rate limiting**: 2 seconds between paginated requests, 1 second between search terms
- **Average execution**: ~2-5 minutes per city (depending on results)
- **Lambda timeout**: 300 seconds (5 minutes)

### Memory
- **Allocated**: 512 MB
- **Typical usage**: 200-300 MB

### Cost Estimation (per city)
- **Google Places API**: ~2,200 units × $0.017/1000 = $0.037
- **Lambda**: ~3 minutes × $0.0000166667/GB-second × 0.5GB = $0.0015
- **DynamoDB**: Write costs vary by data volume
- **Total per city**: ~$0.04-0.05

## Integration with City Collector

The `city_collector` function validates city/state and sends messages to SQS queue:

```python
sqs_client.send_message(
    QueueUrl=os.environ.get('SCRAPER_TASK_QUEUE_URL'),
    MessageBody=json.dumps({
        'city': 'SÃO PAULO',
        'state': 'SP',
        'niche': 'aasi',
    })
)
```

The `data_scrapper` automatically processes these messages and returns batch item failures for retry handling.

## Monitoring

### CloudWatch Metrics
- Lambda invocations
- Lambda duration
- Lambda errors
- SQS message age
- DLQ message count

### Custom Metrics (via logs)
- Quota usage per execution
- Places collected per city
- Duplicate detection rate
- Database operation counts
- API error rates

### Alarms
- DLQ messages: Alert when scraping failures occur
- Lambda errors: Alert on function errors
- Long execution: Alert on timeouts

## Example Usage

### Manual Test Invocation
```bash
aws lambda invoke \
  --function-name backend-core-dev-data_scrapper \
  --payload '{"Records":[{"messageId":"test-123","body":"{\"city\":\"SÃO PAULO\",\"state\":\"SP\",\"niche\":\"aasi\"}"}]}' \
  response.json
```

### Local Testing
```python
from src.models.scrappers.aasi_scrapper import InformationScrapper

scrapper = InformationScrapper(
    niche='aasi',
    api_key='YOUR_API_KEY',
    daily_quota_limit=20000
)

scrapper.collect_places(city='SÃO PAULO', state='SP')

print(f"Status: {scrapper.ensamble['status']}")
print(f"Places: {len(scrapper.ensamble['places'])}")
print(f"Quota used: {scrapper.quota_used}")
```

## Troubleshooting

### No places collected
- Check if search terms exist for niche in `niche_terms.json`
- Verify city/state spelling matches Brazilian naming
- Check Google Places API key validity

### Quota exceeded quickly
- Reduce `GOOGLE_PLACES_DAILY_QUOTA_LIMIT` for testing
- Process fewer cities per day
- Consider upgrading Google Places API plan

### Database errors
- Verify DynamoDB tables exist: `dev-auris-core-places`, `dev-auris-core-companies`
- Check IAM permissions for Lambda role
- Verify table schemas match expected structure

### Duplicate places
- Check if 50-meter threshold is appropriate for dense areas
- Review deduplication logs for distance calculations
- Consider adjusting `DUPLICATE_DISTANCE_THRESHOLD_METERS`
