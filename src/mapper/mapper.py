import aws_lambda_logging
import boto3
import botocore
import gzip
import json
import logging
import os
import pandas as pd
import warnings

from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all
from pandas import json_normalize

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
output_bucket = os.getenv('OUTPUT_BUCKET')
log_level = os.getenv('LOG_LEVEL')

log = logging.getLogger()


def download_data(filename):
    """Download a file from S3
    Parameters
    ----------
    filename: string, required
        Name of the file in S3 source bucket (OpenAQ)
    Returns
    -------
    data_file: string
        Local path to downloaded file
    """

    xray_recorder.begin_subsegment('## download_data_file')
    subsegment = xray_recorder.current_subsegment()
    subsegment.put_metadata('filename', f's3://{src_bucket}/{filename}')

    try:
        data_file = os.path.join('/tmp', os.path.basename(filename))
        s3.download_file(src_bucket, filename, data_file)
        subsegment.put_annotation('DATA_DOWNLOAD', 'SUCCESS')
    except botocore.exceptions.ClientError as e:
        subsegment.put_annotation('DATA_DOWNLOAD', 'FAILURE')
        log.error(f'Unable to download data: {filename}')
        log.debug(e)
        raise
    xray_recorder.end_subsegment()
    return data_file


def process_data(dataframes):
    """Combine datasets and process to extract required fields
    Parameters
    ----------
    dataframes: list of Pandas dataframes, required
        List of dataframes with raw air quality data
    Returns
    -------
    parameter_readings: Pandas dataframe
        Processed dataframe of air quality ratings
    """

    xray_recorder.begin_subsegment('## map')
    subsegment = xray_recorder.current_subsegment()
    try:
        # combine into single dataframe
        data = pd.concat(dataframes, sort=False)
        subsegment.put_metadata('rows', len(data))

        # keep only coulumns we need
        columns_to_keep = ['country','city','location','parameter','value', 'unit','date.utc']
        data.drop(set(data.columns.values) - set(columns_to_keep), axis=1, inplace=True)
        log.info(f"Total rows to process: {len(data)}")

        # pivot to convert air quality parameters to columns
        parameter_readings = data.pivot_table(
            index=[
                'country',
                'city',
                'location',
                'date.utc'],
            columns='parameter',
            values='value').reset_index()
        subsegment.put_annotation('MAP', 'SUCCESS')
    except Exception as e:
        subsegment.put_annotation('MAP', 'FAILURE')
        log.error("Error processing data")
        log.debug(e)

    xray_recorder.end_subsegment()
    return parameter_readings


def upload_intermediate_results(results):
    """Upload a file to S3
    Parameters
    ----------
    results: string, required
        Name of the local file with intermediate results
    """

    xray_recorder.begin_subsegment('## upload_intermediate_results')
    subsegment = xray_recorder.current_subsegment()
    subsegment.put_metadata('filename', f's3://{src_bucket}/lambda-etl-refarch/temp/{results}')

    results_path = os.path.join('/tmp', results)
    # upload to target S3 bucket
    try:
        response = s3.upload_file(
            results_path,
            output_bucket,
            'lambda-etl-refarch/temp/{}'.format(results))
        log.info(f"Uploaded intermediate results to s3://{output_bucket}/lambda-etl-refarch/temp/{results}")
        subsegment.put_annotation('INTERMEDITE_RESULTS_UPLOAD', 'SUCCESS')
    except botocore.exceptions.ClientError as e:
        subsegment.put_annotation('INTERMEDITE_RESULTS_UPLOAD', 'FAILURE')
        log.error(f'Unable to upload intermediate results: {results}')
        log.debug(e)
        raise
    xray_recorder.end_subsegment()


def lambda_handler(event, context):
    aws_lambda_logging.setup(level=log_level,
                             aws_request_id=context.aws_request_id)
    dataframes = []
    # download files locally
    for filename in event:
        data_file = download_data(filename)
        # read each file and store as Pandas dataframe
        with gzip.open(data_file, 'rb') as ndjson_file:
            records = map(json.loads,ndjson_file)
            df = pd.DataFrame.from_records(json_normalize(records))
            dataframes.append(df)

    # process the data to get air quality readings
    parameter_readings = process_data(dataframes)

    # write to file
    results_filename = "{}.json.gz".format(context.aws_request_id)
    parameter_readings.to_json(
        os.path.join(
            '/tmp',
            results_filename),
        compression='gzip')

    # upload to target S3 bucket
    upload_intermediate_results(results_filename)

    # return temp file and number of rows processed.
    return {
        "message": "Mapper phase complete.",
        "processed_file": 'lambda-etl-refarch/temp/{}'.format(results_filename),
        "rows": len(parameter_readings)}
