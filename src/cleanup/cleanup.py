import aws_lambda_logging
import boto3
import logging
import os
import warnings

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

output_bucket = os.getenv('OUTPUT_BUCKET')
log_level = os.getenv('LOG_LEVEL')

log = logging.getLogger()


def delete_intermediate_results(intermediate_files):
    """Delete files from S3
    Parameters
    ----------
    intermediate_files: list, required
        List of S3 files with intemediate results
    """

    try:
        log.info(
            s3.delete_objects(
                Bucket=output_bucket,
                Delete={
                    'Objects': intermediate_files}))
    except botocore.exceptions.ClientError as e:
        log.error(f'Unable to delete intermediate results')
        log.debug(e)
        raise

def lambda_handler(event, context):
    aws_lambda_logging.setup(level=log_level,
                             aws_request_id=context.aws_request_id)
    for item in event:
        intermediate_files = event['intermediate_files']

    # delete from S3 bucket
    delete_intermediate_results(intermediate_files)

    return {
        "message": event["message"], 
        "results": f'Download results from {event["output_file"]}'}
