# API Key Authentication

This project uses API keys to secure access to the API endpoints. The API keys are managed through AWS API Gateway and are required for all API requests.

## Setting Up API Keys

### 1. Create API Key in AWS Cognito

While this example uses a simple API key from environment variables, in production you might want to use AWS Cognito for more robust authentication:

1. Go to AWS Cognito in your AWS Console
2. Create a User Pool or use an existing one
3. Create an app client for API access
4. Generate API keys through the app client

### 2. Store API Key in GitHub Secrets

For CI/CD deployment, the API key is stored as a GitHub Secret:

1. Go to your GitHub repository
2. Navigate to Settings > Secrets and variables > Actions
3. Add a new repository secret named `API_KEY` with the value of your API key

### 3. Local Development

For local development, you can store the API key in your local environment:

```bash
# Linux/macOS
export API_KEY=your-api-key-value

# Windows
set API_KEY=your-api-key-value
```

## Using the API Key

When making requests to the API endpoints, include the API key in the `x-api-key` header:

```bash
curl -H "x-api-key: your-api-key-value" https://your-api-endpoint.execute-api.us-east-1.amazonaws.com/dev/hello
```

## Serverless Configuration

The API key is configured in the `serverless.yml` file:

```yaml
provider:
  apiGateway:
    apiKeys:
      - name: ${self:service}-${self:provider.stage}-key
        description: API key for ${self:service} service in ${self:provider.stage} stage
        value: ${env:API_KEY, ''}
```

## Security Considerations

- Never commit API keys to version control
- Rotate API keys periodically
- Consider using more robust authentication methods like JWT tokens for production use
- Set appropriate throttling and usage plans in API Gateway
