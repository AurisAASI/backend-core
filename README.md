# Backend Core

A serverless backend service using AWS Lambda and Python 3.10.

## Project Structure

```
backend-core/
├── .github/                 # GitHub Actions workflows
│   └── workflows/
│       └── ci.yml           # CI/CD pipeline
├── .gitignore               # Git ignore file
├── .vscode/                 # VS Code settings
│   └── settings.json        # Editor configuration for Blue formatting
├── code_quality.sh          # Code quality automation script
├── package.json             # NPM dependencies for Serverless Framework
├── pyproject.toml           # Python tooling configuration (Blue, isort, pytest)
├── README.md                # This file
├── requirements.txt         # Python dependencies
├── serverless.yml           # Serverless Framework configuration
├── src/                     # Source code
│   ├── functions/           # Lambda functions
│   │   ├── city_collector/  # City data collection function
│   │   │   ├── __init__.py
│   │   │   ├── handler.py   # Lambda handler
│   │   │   └── README.md    # Function documentation
│   │   └── aasi_scrapper/   # AASI data scraping function
│   │       ├── __init__.py
│   │       └── handler.py   # Lambda handler
│   └── shared/              # Shared code between functions
│       ├── __init__.py
│       ├── aws_utils.py     # AWS utility functions
│       └── utils.py         # Common utility functions
└── tests/                   # Test files
    ├── __init__.py
    └── test_city_collector.py  # Tests for city_collector function (100% coverage)
```

## Prerequisites

- [Python 3.10](https://www.python.org/downloads/)
- [Node.js](https://nodejs.org/) (for Serverless Framework)
- [AWS CLI](https://aws.amazon.com/cli/) (configured with credentials)
- [Serverless Framework](https://www.serverless.com/)

## Setup

1. Install Serverless Framework and plugins:
   ```bash
   npm install
   ```

2. Create a virtual environment and install dependencies:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

## Environment Configuration

This project supports multiple environments (dev/prod) with centralized configuration management.

### Environment Variables

Create a `.env` file in the project root with the following variables:

```bash
# AWS Configuration
AWS_ACCOUNT_ID=your-account-id
ALARM_EMAIL=your-email@example.com

# Google Places API Keys (Stage-specific)
GOOGLE_PLACES_API_KEY_DEV=your-dev-api-key
GOOGLE_PLACES_API_KEY_PROD=your-prod-api-key

# Google Places API Daily Quota Limits
GOOGLE_PLACES_DAILY_QUOTA_LIMIT_DEV=10000
GOOGLE_PLACES_DAILY_QUOTA_LIMIT_PROD=20000
```

### Centralized Settings

The project uses `src/shared/settings.py` for centralized configuration management:

- **Automatic stage detection**: Reads `STAGE` environment variable (injected by Serverless Framework)
- **Dynamic resource naming**: All AWS resources are prefixed with stage (`dev-` or `prod-`)
- **Stage-specific API keys**: Automatically selects the correct Google API key per environment
- **Configurable quotas**: Different quota limits for dev and prod environments

### Resource Naming Convention

All AWS resources follow the pattern: `{stage}-auris-core-{resource-type}`

**Development (dev):**
- DynamoDB: `dev-auris-core-companies`, `dev-auris-core-places`
- Lambda: `backend-core-dev-city_collector`, `backend-core-dev-data_scrapper`
- SQS: `backend-core-dev-scraper-tasks`

**Production (prod):**
- DynamoDB: `prod-auris-core-companies`, `prod-auris-core-places`
- Lambda: `backend-core-prod-city_collector`, `backend-core-prod-data_scrapper`
- SQS: `backend-core-prod-scraper-tasks`

### DynamoDB Tables

DynamoDB tables are automatically created by Serverless Framework with:
- **DeletionPolicy: Retain** - Tables won't be deleted if stack is removed
- **UpdateReplacePolicy: Retain** - Tables won't be replaced during updates
- **Pay-per-request billing** - No capacity planning needed

**Companies Table:**
- Primary Key: `companyID` (String)
- Contains: Company metadata, collection status, niche information

**Places Table:**
- Primary Key: `placeID` (String)
- Global Secondary Index: `companyID-index` for efficient company-to-places lookups
- Contains: Google Places data, links to company records

## Local Development

Run the API locally using Serverless Offline:
```bash
npm run offline
```

## Code Quality and Formatting

We use [Blue](https://blue.readthedocs.io/) for code formatting and linting. Blue is an opinionated formatter that provides both formatting and basic linting capabilities in one tool.

### Quick Commands

Use our code quality script for all formatting and linting tasks:

```bash
# Check code quality (formatting and basic linting)
./code_quality.sh lint

# Format code automatically
./code_quality.sh format

# Run tests with coverage
./code_quality.sh test

# Check everything (lint + test)
./code_quality.sh all

# Fix formatting issues and run tests
./code_quality.sh all-fix

# Show available commands
./code_quality.sh help
```

### Manual Commands

If you prefer to run tools individually:

```bash
# Check formatting without making changes
blue --check src tests

# Format code automatically
blue src tests

# Check import sorting
isort --check-only --diff src tests

# Fix import sorting
isort src tests
```

### What Blue Does

Blue automatically handles:
- ✅ **Code formatting** (line length, indentation, spacing)
- ✅ **Quote normalization** (prefers single quotes)
- ✅ **Import organization** (when used with isort)
- ✅ **Basic linting** (PEP 8 compliance)

### Configuration

Blue configuration is in [`pyproject.toml`](pyproject.toml):
- **Line length**: 88 characters
- **Target Python version**: 3.10
- **Single quotes preferred**: More Pythonic style

## Testing

We use [pytest](https://pytest.org/) for testing with comprehensive coverage reporting.

### Quick Testing Commands

```bash
# Run tests with coverage
./code_quality.sh test

# Run all quality checks and tests
./code_quality.sh all

# Fix formatting and run tests
./code_quality.sh all-fix
```

### Manual Testing Commands

```bash
# Run all tests
pytest tests/

# Run tests with coverage report
pytest tests/ --cov=src/ --cov-report=html --cov-report=term-missing

# Run specific test file
pytest tests/test_city_collector.py -v

# Run with verbose output
pytest tests/ -v
```

### Coverage Reports

After running tests with coverage, you can view the HTML report:
```bash
# Coverage report is generated in htmlcov/index.html
open htmlcov/index.html  # macOS
# or
xdg-open htmlcov/index.html  # Linux
```

## Deployment

### Prerequisites - JWT Secret Setup

**IMPORTANT:** Before deploying for the first time, you must set up the JWT secret in AWS Secrets Manager:

```bash
# For development
npm run setup:jwt:dev

# For production
npm run setup:jwt:prod
```

This will:
- Generate a cryptographically secure 64-character random secret
- Store it in AWS Secrets Manager as `{stage}-auris-jwt-secret`
- Configure automatic rotation every 6 months (180 days)
- Enable Lambda functions to fetch the secret at runtime

See [scripts/README.md](scripts/README.md) for more details on JWT secret management.

### Deploy to Development

Deploys to `dev` stage with dev-specific resources and configuration:
```bash
npm run deploy:dev
```

This will:
- Create/update `dev-auris-core-*` DynamoDB tables
- Deploy Lambda functions with dev API keys
- Use dev quota limits (10,000/day default)
- Configure access to `dev-auris-jwt-secret` in Secrets Manager

### Deploy to Production

Deploys to `prod` stage with production resources and configuration:
```bash
npm run deploy:prod
```

This will:
- Create/update `prod-auris-core-*` DynamoDB tables
- Deploy Lambda functions with prod API keys
- Use prod quota limits (20,000/day default)

**Important:** Ensure you have configured `GOOGLE_PLACES_API_KEY_PROD` in your `.env` file before deploying to production.

## Remove Stack

Remove from dev environment:
```bash
npm run remove:dev
```

Remove from production:
```bash
npm run remove:prod
```

## Adding a New Lambda Function

1. **Create function structure:**
   ```bash
   mkdir -p src/functions/new_function
   touch src/functions/new_function/__init__.py
   touch src/functions/new_function/handler.py
   ```

2. **Add handler code:** Implement your Lambda function in `handler.py`

3. **Update serverless.yml:** Add the new function definition

4. **Create tests:**
   ```bash
   touch tests/test_new_function.py
   ```

5. **Run quality checks:**
   ```bash
   ./code_quality.sh all-fix
   ```

6. **Verify everything works:**
   ```bash
   ./code_quality.sh all
   ```