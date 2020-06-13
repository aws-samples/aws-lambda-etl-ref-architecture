import aws_lambda_logging
import boto3
import logging
import os
import warnings

from datetime import datetime, timedelta
from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all

# ref:https://github.com/jmespath/jmespath.py/issues/187 
warnings.filterwarnings(
    action='ignore',
    category=SyntaxWarning,
    module=r'jmespath\.visitor'
)

patch_all()

# initialise static variables
s3 = boto3.client('s3')

src_bucket = os.getenv('SOURCE_BUCKET')
chunk_size = int(os.getenv('CHUNK_SIZE'))
log_level = os.getenv('LOG_LEVEL')

# look for all files from previous day
prev_day = datetime.utcnow() - timedelta(days=1)
prev_day = prev_day.strftime('%Y-%m-%d')
prefix = '{}/{}/'.format(os.getenv('PREFIX'), prev_day)

log = logging.getLogger()


def get_file_inventory():
    """List files in OpenAQ bucket for previous day
    Returns
    -------
    file_names: list
        List of air quality files to be processed
    """

    try:
        response = s3.list_objects_v2(Bucket=src_bucket, Prefix=prefix)
        file_names = [item['Key'] for item in response['Contents']]
        log.info(f"Total files to process: {len(file_names)}")
    except Exception as e:
        log.error(f'Unable to list OpenAQ files: {response}')
        log.debug(e)
        raise
    return file_names

def lambda_handler(event, context):
    aws_lambda_logging.setup(level=log_level,
                             aws_request_id=context.aws_request_id)
    log.info(f"Processing data for: {prev_day}")

    file_names = get_file_inventory()
    chunks = [file_names[i:i + chunk_size]
            for i in range(0, len(file_names), chunk_size)]

    return {
        "chunks": chunks, 
        "message": "Init phase complete"}
