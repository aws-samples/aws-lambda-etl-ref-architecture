import aws_lambda_logging
import boto3
import botocore
import gzip
import json
import logging
import os
import numpy as np
import pandas as pd
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

output_bucket = os.environ['OUTPUT_BUCKET']
log_level = os.getenv('LOG_LEVEL')

prev_day = datetime.utcnow() - timedelta(days=1)
prev_day = prev_day.strftime('%Y-%m-%d')

log = logging.getLogger()


def download_intermediate_results(filename):
    """Download a file from S3
    Parameters
    ----------
    filename: string, required
        Name of the file in S3 source bucket (OpenAQ)
    Returns
    -------
    processed_file: string
        Local path to downloaded file
    """

    xray_recorder.begin_subsegment('## download_data_file')
    subsegment = xray_recorder.current_subsegment()
    subsegment.put_metadata('filename', f's3://{output_bucket}/{filename}')

    try:
        processed_file = os.path.join(
            '/tmp', os.path.basename(filename))
        s3.download_file(
            output_bucket,
            filename,
            processed_file)
        subsegment.put_annotation('DATA_DOWNLOAD', 'SUCCESS')
    except botocore.exceptions.ClientError as e:
        subsegment.put_annotation('DATA_DOWNLOAD', 'FAILURE')
        log.error(f'Unable to download rsult file: {filename}')
        log.debug(e)
        raise
    xray_recorder.end_subsegment()
    return processed_file


def process_intermediate_results(dataframes):
    """Combine hourly air quality ratings and calculate daily ratings for each location.
    Parameters
    ----------
    dataframes: list of Pandas dataframes, required
        List of dataframes with hourly air quality ratings
    Returns
    -------
    summary_stats: Pandas dataframe
        Daily summary of air quality ratings
    """

    xray_recorder.begin_subsegment('## reduce')
    subsegment = xray_recorder.current_subsegment()
 
    try:
        # combine into single dataframe
        data = pd.concat(dataframes, sort=False)

        subsegment.put_metadata('rows', len(data))
        data['date.utc'] = pd.to_datetime(
            data['date.utc'], utc=True)

        # calculate stats
        summary_stats = data.set_index('date.utc').groupby([pd.Grouper(
            freq='D'), 'country', 'city', 'location']).agg([np.nanmin, np.nanmax, np.nanmean])
        summary_stats.columns = ["_".join(x)
                                 for x in summary_stats.columns.ravel()]

        # format the columns
        summary_stats = summary_stats.reset_index()
        # there is occasionally historic data in the source
        summary_stats = summary_stats[summary_stats['date.utc'].dt.date.astype(str) == prev_day]
        summary_stats['date.utc'] = summary_stats['date.utc'].dt.date
        summary_stats.drop_duplicates(inplace=True)
        new_columns = {'date.utc': 'date',
                       'bc_nanmin': 'bc_min',
                       'bc_nanmax': 'bc_max',
                       'bc_nanmean': 'bc_mean',
                       'co_nanmin': 'co_min',
                       'co_nanmax': 'co_max',
                       'co_nanmean': 'co_mean',
                       'no2_nanmin': 'no2_min',
                       'no2_nanmax': 'no2_max',
                       'no2_nanmean': 'no2_mean',
                       'o3_nanmin': 'o3_min',
                       'o3_nanmax': 'o3_max',
                       'o3_nanmean': 'o3_mean',
                       'pm10_nanmin': 'pm10_min',
                       'pm10_nanmax': 'pm10_max',
                       'pm10_nanmean': 'pm10_mean',
                       'pm25_nanmin': 'pm25_min',
                       'pm25_nanmax': 'pm25_max',
                       'pm25_nanmean': 'pm25_mean',
                       'so2_nanmin': 'so2_min',
                       'so2_nanmax': 'so2_max',
                       'so2_nanmean': 'so2_mean'
                       }
        summary_stats.rename(columns=new_columns, inplace=True)
        subsegment.put_annotation('REDUCE', 'SUCCESS')
    except Exception as e:
        subsegment.put_annotation('REDUCE', 'FAILURE')
        log.error("Error processing data")
        log.debug(e)
        raise

    xray_recorder.end_subsegment()
    return summary_stats


def upload_final_results(results):
    """Upload a file to S3
    Parameters
    ----------
    results: string, required
        Name of the local file with final results
    """

    xray_recorder.begin_subsegment('## upload_final_results')
    subsegment = xray_recorder.current_subsegment()
    subsegment.put_metadata('filename', f's3://{output_bucket}/lambda-etl-refarch/output/{results}')

    results_path = os.path.join('/tmp', results)
    # upload to target S3 bucket
    try:
        response = s3.upload_file(
            results_path,
            output_bucket,
            'lambda-etl-refarch/output/{}'.format(results))
        log.info(f"Uploaded final results to s3://{output_bucket}/lambda-etl-refarch/output/{results}")
        subsegment.put_annotation('FINAL_RESULTS_UPLOAD', 'SUCCESS')
    except botocore.exceptions.ClientError as e:
        subsegment.put_annotation('FINAL_RESULTS_UPLOAD', 'FAILURE')
        log.error(f'Unable to upload final results: {results}')
        log.debug(e)
        raise
    xray_recorder.end_subsegment()


def lambda_handler(event, context):
    aws_lambda_logging.setup(level=log_level,
                             aws_request_id=context.aws_request_id)
    dataframes = []
    temp_files = []
    # download files locally
    for item in event:
        temp_files.append({'Key': item['processed_file']})
        intermediate_result = download_intermediate_results(item['processed_file'])
        # read each file and store as Pandas dataframe
        with gzip.GzipFile(intermediate_result, 'r') as data_file:
            raw_json = json.loads(data_file.read())
            df = pd.DataFrame.from_dict(raw_json)
            dataframes.append(df)

    summary_stats = process_intermediate_results(dataframes)
    # write to file
    output_file_name = '{}.csv.gz'.format(prev_day)
    output_file = '/tmp/{}'.format(output_file_name)
    summary_stats.to_csv(
        output_file,
        compression='gzip',
        index=False,
        header=True)

    upload_final_results(output_file_name)

    return {
        "message": "[lambda-etl-refarch] Successfully processed data for {}".format(prev_day),
        "intermediate_files": temp_files,
        "output_file": f's3://{output_bucket}/lambda-etl-refarch/output/{output_file_name}'}
