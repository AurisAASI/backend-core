#!/usr/bin/env python3
"""
Script to generate and store JWT secret key in AWS Secrets Manager.

This script should be run ONCE before deploying the application to ensure
the JWT secret exists in AWS Secrets Manager. It handles the case where
the secret already exists gracefully.

Usage:
    python scripts/setup_jwt_secret.py --stage dev
    python scripts/setup_jwt_secret.py --stage prod
    python scripts/setup_jwt_secret.py --stage dev --regenerate  # Rotate the secret

IMPORTANT:
    - This MUST be executed BEFORE running 'npm run deploy:dev' or 'npm run deploy:prod'
    - Run this once per stage to initialize the JWT secret
    - The secret is managed OUTSIDE of CloudFormation to persist across deployments
    - If you need to regenerate (rotate), use the --regenerate flag
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


def create_or_update_secret(secret_name: str, secret_value: str, region: str = 'us-east-1', regenerate: bool = False) -> None:
    """
    Create or update a secret in AWS Secrets Manager.

    Args:
        secret_name: Name of the secret in Secrets Manager
        secret_value: The secret value to store
        region: AWS region (default: us-east-1)
        regenerate: If True, overwrite existing secret without confirmation
    """
    client = boto3.client('secretsmanager', region_name=region)

    # Prepare secret as JSON
    secret_data = {
        'jwt_secret_key': secret_value
    }
    secret_string = json.dumps(secret_data)

    try:
        # Try to describe (check if secret exists)
        try:
            client.describe_secret(SecretId=secret_name)
            secret_exists = True
        except client.exceptions.ResourceNotFoundException:
            secret_exists = False

        if secret_exists:
            if regenerate:
                # Overwrite existing secret
                response = client.put_secret_value(
                    SecretId=secret_name,
                    SecretString=secret_string
                )
                print(f"✅ Secret rotated successfully: {secret_name}")
                print(f"   ARN: {response['ARN']}")
                print(f"   Version: {response['VersionId']}")
                print(f"\n⚠️  WARNING: All existing JWT tokens will be invalid after rotation!")
                print(f"   Make sure all clients refresh their tokens.")
            else:
                # Secret already exists, skip
                print(f"ℹ️  Secret already exists: {secret_name}")
                print(f"   Skipping creation to avoid conflicts with existing deployments.")
                print(f"\n✅ JWT secret is ready to use!")
                print(f"\n   To regenerate/rotate the secret (every 6 months for security):")
                print(f"   python scripts/setup_jwt_secret.py --stage {args.stage if 'args' in locals() else 'dev'} --regenerate")
        else:
            # Create new secret
            response = client.create_secret(
                Name=secret_name,
                Description='JWT secret key for token signing and validation',
                SecretString=secret_string,
                Tags=[
                    {'Key': 'Service', 'Value': 'backend-core'},
                    {'Key': 'ManagedBy', 'Value': 'setup_jwt_secret.py'},
                    {'Key': 'CreatedAt', 'Value': 'deployment-init'},
                ]
            )
            print(f"✅ Secret created successfully: {secret_name}")
            print(f"   ARN: {response['ARN']}")
            print(f"   Version: {response['VersionId']}")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_msg = e.response['Error']['Message']
        
        if 'ResourceExistsException' in error_code or 'already exists' in error_msg:
            print(f"ℹ️  Secret already exists: {secret_name}")
            print(f"   Skipping creation to avoid conflicts.")
            if not regenerate:
                print(f"\n✅ JWT secret is ready to use!")
        else:
            print(f"❌ Failed to create/update secret: {error_msg}")
            sys.exit(1)


def main():
    """Main function to parse arguments and create/update JWT secret."""
    parser = argparse.ArgumentParser(
        description='Generate and store JWT secret key in AWS Secrets Manager',
        epilog='IMPORTANT: Run this BEFORE deploying (npm run deploy:dev/prod)'
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
        help='Force regenerate/rotate the secret (use every 6 months for security compliance)'
    )

    args = parser.parse_args()

    # Generate secret name
    secret_name = f'{args.stage}-auris-jwt-secret'

    print(f"\n🔐 JWT Secret Manager Setup")
    print(f"{'=' * 60}")
    print(f"Stage:     {args.stage}")
    print(f"Region:    {args.region}")
    print(f"Secret:    {secret_name}")
    print(f"Action:    {'Regenerate/Rotate' if args.regenerate else 'Create or Verify'}")
    print(f"{'=' * 60}\n")

    # Generate JWT secret
    print(f"🔑 Generating JWT secret key...")
    jwt_secret = generate_jwt_secret(args.length)
    print(f"✅ Secret generated ({len(jwt_secret)} characters)")

    # Create or update secret in AWS
    print(f"\n📤 Connecting to AWS Secrets Manager...")
    create_or_update_secret(secret_name, jwt_secret, args.region, args.regenerate)

    print(f"\n" + "=" * 60)
    print(f"✅ JWT secret setup completed successfully!")
    print(f"{'=' * 60}\n")

    if not args.regenerate:
        print(f"📋 Next steps:")
        print(f"   1. Deploy your application:")
        print(f"      npm run deploy:{args.stage}")
        print(f"   2. Lambda functions will automatically fetch the secret")
        print(f"   3. The secret persists across redeployments")
        print(f"\n⏰  Reminder (Security Compliance):")
        print(f"   Rotate/regenerate the JWT secret every 6 months:")
        print(f"   python scripts/setup_jwt_secret.py --stage {args.stage} --regenerate")
    else:
        print(f"🔄 JWT secret has been rotated successfully!")
        print(f"\n⚠️  IMPORTANT:")
        print(f"   - All existing JWT tokens are now INVALID")
        print(f"   - Users must re-authenticate to get new tokens")
        print(f"   - Update any client-side applications immediately")


if __name__ == '__main__':
    main()

