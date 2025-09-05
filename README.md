# Backend Core

A serverless backend service using AWS Lambda and Python 3.10.

## Project Structure

```
backend-core/
├── .gitignore               # Git ignore file
├── .flake8                  # Flake8 configuration
├── package.json             # NPM dependencies for Serverless Framework
├── pyproject.toml           # Python tooling configuration
├── README.md                # This file
├── requirements.txt         # Python dependencies
├── serverless.yml           # Serverless Framework configuration
├── src/                     # Source code
│   ├── functions/           # Lambda functions
│   │   └── hello/           # Example function
│   │       ├── __init__.py
│   │       └── handler.py   # Lambda handler
│   └── shared/              # Shared code between functions
│       ├── __init__.py
│       └── aws_utils.py     # AWS utility functions
└── tests/                   # Test files
    ├── __init__.py
    └── test_hello.py        # Tests for hello function
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

## Testing

Run tests with pytest:
```bash
npm test
# or directly with pytest
pytest
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

1. Create a new folder in `src/functions/`
2. Add the handler code and any supporting files
3. Update `serverless.yml` with the new function definition
4. Add tests in the `tests/` directory