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

Deploy to dev environment:
```bash
npm run deploy:dev
```

Deploy to production:
```bash
npm run deploy:prod
```

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