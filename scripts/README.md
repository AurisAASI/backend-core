# Backend Core Scripts

This directory contains utility scripts for managing the backend infrastructure.

## JWT Secret Management

### Setup JWT Secret

Generate and store a JWT secret key in AWS Secrets Manager.

**Usage:**

```bash
# Development environment
npm run setup:jwt:dev
# or
python scripts/setup_jwt_secret.py --stage dev

# Production environment
npm run setup:jwt:prod
# or
python scripts/setup_jwt_secret.py --stage prod
```

**Options:**

- `--stage`: Deployment stage (required: dev or prod)
- `--region`: AWS region (default: us-east-1)
- `--length`: Secret length in characters (default: 64)
- `--regenerate`: Force regenerate even if secret exists

**What it does:**

1. Generates a cryptographically secure random secret (64 characters by default)
2. Stores it in AWS Secrets Manager as `{stage}-auris-jwt-secret`
3. Configures it for automatic rotation every 6 months (180 days)

**Important Notes:**

- ⚠️ Run this **BEFORE** deploying the application for the first time
- ⚠️ Regenerating the secret will invalidate all existing JWT tokens
- ✅ The secret is automatically rotated every 6 months
- ✅ Lambda functions fetch the secret at runtime (no hardcoding needed)

**Example:**

```bash
# First time setup for development
npm run setup:jwt:dev

# Output:
# 🔐 JWT Secret Manager Setup
# ==================================================
# Stage:  dev
# Region: us-east-1
# Secret: dev-auris-jwt-secret
# ==================================================
# 
# 🔑 Generating JWT secret key...
# ✅ Secret generated (64 characters)
# 
# 📤 Uploading to AWS Secrets Manager...
# ✅ Secret created successfully: dev-auris-jwt-secret
#    ARN: arn:aws:secretsmanager:us-east-1:123456789:secret:dev-auris-jwt-secret-AbCdEf
#    Version: abc123-def456-ghi789
# 
# ✅ JWT secret setup completed successfully!
```

### Rotation Schedule

The JWT secret is configured to rotate automatically:

- **Frequency:** Every 6 months (180 days)
- **Method:** Automatic via AWS Secrets Manager
- **Impact:** Users will need to re-authenticate after rotation
- **Lambda:** Rotation handled by AWS-managed Lambda function

**Manual Rotation:**

If you need to rotate the secret manually (e.g., security breach):

```bash
# Force regenerate the secret
python scripts/setup_jwt_secret.py --stage prod --regenerate
```

⚠️ **Warning:** Manual rotation will immediately invalidate all active sessions!

## Prerequisites

- AWS CLI configured with appropriate credentials
- Python 3.10+ installed
- `boto3` package installed (`pip install boto3`)
- IAM permissions for Secrets Manager:
  - `secretsmanager:CreateSecret`
  - `secretsmanager:PutSecretValue`
  - `secretsmanager:DescribeSecret`
  - `secretsmanager:GetSecretValue`

## Monitoring

Monitor secret rotation in the AWS Console:

1. Go to **AWS Secrets Manager**
2. Select region: `us-east-1` (or your configured region)
3. Find secret: `{stage}-auris-jwt-secret`
4. View **Rotation configuration** tab
5. Check **Last rotation** and **Next rotation** dates

## Troubleshooting

**Secret already exists error:**

```bash
# Use --regenerate flag
python scripts/setup_jwt_secret.py --stage dev --regenerate
```

**Permission denied:**

Ensure your AWS credentials have the required Secrets Manager permissions.

**Lambda can't access secret:**

1. Check IAM role has `secretsmanager:GetSecretValue` permission
2. Verify secret name matches: `{stage}-auris-jwt-secret`
3. Check the secret exists in the correct region
