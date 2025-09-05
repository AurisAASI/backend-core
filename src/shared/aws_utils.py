"""
Common utility functions for AWS services
"""
from typing import Any, Dict, Optional

import boto3


def get_boto_client(service_name: str, region_name: Optional[str] = None) -> Any:
    """
    Get a boto3 client for a specific AWS service

    Args:
        service_name (str): Name of the AWS service (e.g. 's3', 'dynamodb')
        region_name (Optional[str]): AWS region name. If None, uses the default region.

    Returns:
        Any: boto3 client for the specified service
    """
    if region_name:
        return boto3.client(service_name, region_name=region_name)
    return boto3.client(service_name)


def get_boto_resource(service_name: str, region_name: Optional[str] = None) -> Any:
    """
    Get a boto3 resource for a specific AWS service

    Args:
        service_name (str): Name of the AWS service (e.g. 's3', 'dynamodb')
        region_name (Optional[str]): AWS region name. If None, uses the default region.

    Returns:
        Any: boto3 resource for the specified service
    """
    if region_name:
        return boto3.resource(service_name, region_name=region_name)
    return boto3.resource(service_name)
