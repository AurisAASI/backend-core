#!/usr/bin/env python3
"""
Script to generate and store JWT secret key in AWS Secrets Manager.

This script should be run once before deploying the application to ensure
the JWT secret exists in AWS Secrets Manager.

Usage:
    python scripts/setup_jwt_secret.py --stage dev
    python scripts/setup_jwt_secret.py --stage prod
"""

import argparse
import json
import secrets
import sys

import boto3
from botocore.exceptions import ClientError


def generate_jwt_secret(length: int = 64) -> str:
    """
    Generate a cryptographically secure JWT secret key.

    Args:
        length: Length of the secret (default: 64 characters)

    Returns:
        URL-safe base64-encoded random string
    """
    return secrets.token_urlsafe(length)


def create_or_update_secret(secret_name: str, secret_value: str, region: str = 'us-east-1') -> None:
    """
    Create or update a secret in AWS Secrets Manager.

    Args:
        secret_name: Name of the secret in Secrets Manager
        secret_value: The secret value to store
        region: AWS region (default: us-east-1)
    """
    client = boto3.client('secretsmanager', region_name=region)

    # Prepare secret as JSON
    secret_data = {
        'jwt_secret_key': secret_value
    }
    secret_string = json.dumps(secret_data)

    try:
        # Try to create the secret
        response = client.create_secret(
            Name=secret_name,
            Description='JWT secret key for token signing and validation',
            SecretString=secret_string,
            Tags=[
                {'Key': 'Service', 'Value': 'backend-core'},
                {'Key': 'ManagedBy', 'Value': 'setup_jwt_secret.py'},
            ]
        )
        print(f"✅ Secret created successfully: {secret_name}")
        print(f"   ARN: {response['ARN']}")
        print(f"   Version: {response['VersionId']}")

    except client.exceptions.ResourceExistsException:
        # Secret already exists, update it
        print(f"ℹ️  Secret already exists: {secret_name}")
        try:
            response = client.put_secret_value(
                SecretId=secret_name,
                SecretString=secret_string
            )
            print(f"✅ Secret updated successfully")
            print(f"   ARN: {response['ARN']}")
            print(f"   Version: {response['VersionId']}")
        except ClientError as e:
            print(f"❌ Failed to update secret: {str(e)}")
            sys.exit(1)

    except ClientError as e:
        print(f"❌ Failed to create secret: {str(e)}")
        sys.exit(1)


def main():
    """Main function to parse arguments and create/update JWT secret."""
    parser = argparse.ArgumentParser(
        description='Generate and store JWT secret key in AWS Secrets Manager'
    )
    parser.add_argument(
        '--stage',
        required=True,
        choices=['dev', 'prod'],
        help='Deployment stage (dev or prod)'
    )
    parser.add_argument(
        '--region',
        default='us-east-1',
        help='AWS region (default: us-east-1)'
    )
    parser.add_argument(
        '--length',
        type=int,
        default=64,
        help='Length of the generated secret (default: 64)'
    )
    parser.add_argument(
        '--regenerate',
        action='store_true',
        help='Force regenerate the secret even if it exists'
    )

    args = parser.parse_args()

    # Generate secret name
    secret_name = f'{args.stage}-auris-jwt-secret'

    print(f"\n🔐 JWT Secret Manager Setup")
    print(f"{'=' * 50}")
    print(f"Stage:  {args.stage}")
    print(f"Region: {args.region}")
    print(f"Secret: {secret_name}")
    print(f"{'=' * 50}\n")

    # Check if secret exists
    client = boto3.client('secretsmanager', region_name=args.region)
    secret_exists = False

    try:
        client.describe_secret(SecretId=secret_name)
        secret_exists = True
    except client.exceptions.ResourceNotFoundException:
        pass

    if secret_exists and not args.regenerate:
        print(f"⚠️  Secret already exists: {secret_name}")
        print(f"   Use --regenerate flag to force regeneration (this will invalidate all existing JWT tokens)")
        response = input("\nDo you want to regenerate? (yes/no): ")
        if response.lower() not in ['yes', 'y']:
            print("❌ Operation cancelled")
            sys.exit(0)

    # Generate JWT secret
    print(f"🔑 Generating JWT secret key...")
    jwt_secret = generate_jwt_secret(args.length)
    print(f"✅ Secret generated ({len(jwt_secret)} characters)")

    # Create or update secret in AWS
    print(f"\n📤 Uploading to AWS Secrets Manager...")
    create_or_update_secret(secret_name, jwt_secret, args.region)

    print(f"\n✅ JWT secret setup completed successfully!")
    print(f"\n⚠️  IMPORTANT:")
    print(f"   - The secret is now managed by AWS Secrets Manager")
    print(f"   - Automatic rotation is configured for every 6 months (180 days)")
    print(f"   - All Lambda functions will fetch this secret at runtime")
    print(f"   - Do NOT store the secret in environment variables or .env files")
    print(f"\n📝 Next steps:")
    print(f"   1. Deploy your serverless application: serverless deploy --stage {args.stage}")
    print(f"   2. Lambda functions will automatically fetch the secret")
    print(f"   3. Monitor rotation in AWS Secrets Manager console")


if __name__ == '__main__':
    main()
