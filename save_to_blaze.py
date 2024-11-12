from b2_authentication import get_authenticated_b2_api
import boto3

import boto3  # REQUIRED! - Details here: https://pypi.org/project/boto3/
from botocore.exceptions import ClientError
from botocore.config import Config
from dotenv import load_dotenv  # Project Must install Python Package:  python-dotenv
import os
import sys

# Initialize once at the start of your pipeline
b2_api = get_authenticated_b2_api()

# def get_b2_resource(endpoint, key_id="", application_key=""):
#     b2 = boto3.resource(service_name='s3',
#                         endpoint_url=endpoint,                # Backblaze endpoint
#                         aws_access_key_id=key_id,              # Backblaze keyID
#                         aws_secret_access_key=application_key, # Backblaze applicationKey
#                         config = Config(
#                             signature_version='s3v4',
#                     ))
#     return b2


