import urllib.request
import xmltodict
import json
import boto3
from datetime import datetime
import os
import requests


def update_nytimes_airbyte_source(run_year, run_month):
    url = os.environ.get('AIRBYTE_URL')
    auth = os.environ.get('AIRBYTE_AUTH')
    source_id = os.environ.get('AIRBYTE_NYT_SRC_ID')
    nyt_api_key = os.environ.get('NYT_API_KEY')
    header = {
        'Authorization': f"Basic {auth}"
    }

    source_update = 'sources/update'

    update_payload = {
        "sourceId": source_id,
        "connectionConfiguration": {
            "period": 30,
            "api_key": nyt_api_key,
            "end_date": f"{run_year}-{run_month}",
            "start_date": f"{run_year}-{run_month}"
        },
        "name": "New York Times"
    }

    try:
        r = requests.post(
            url + source_update,
            headers=header,
            json=update_payload
        )
        return True
    except Exception as e:
        print('Update of New York Times source failed:', e)
        return False


def get_arxiv_json_data(
        endpoint: str,
        query: str,
        start: str,
        max_results: str,
        sort_by: str,
        sort_order: str
) -> str:
    """

      :param endpoint:
      :param query:
      :param start:
      :param max_results:
      :param sort_by:
      :param sort_order:
      :return:
      """
    url = f"{endpoint}?" \
          f"search_query={query}&" \
          f"start={start}&" \
          f"max_results={max_results}&" \
          f"sortBy={sort_by}&" \
          f"sortOrder={sort_order}"
    data = urllib.request.urlopen(url)
    if data:
        xml_resp = data.read().decode('utf-8')
        resp = xmltodict.parse(xml_resp)
        return json.dumps(resp)


def aws_s3_writer(
        data: bytes,
        bucket: str,
        filename: str
):
    """
      :param data:
      :param bucket:
      :param filename:
      :return:
      """
    try:
        session = boto3.Session()
        s3 = session.client('s3')
        now_ts = datetime.now()
        if data and len(filename) > 0:
            object = s3.put_object(
                Bucket=bucket,
                Key=f'bronze/arxiv/{now_ts}_{filename}.json',
                Body=data
            )
    except Exception as e:
        raise RuntimeError('Invalid AWS credentials', e)


def arxiv_extract_load():
    endpoint = os.environ.get('ARXIV_ENDPOINT')
    query = os.environ.get('ARXIV_QUERY')
    start = os.environ.get('ARXIV_START')
    max_results = os.environ.get('ARXIV_MAX_RESULTS')
    sort_by = os.environ.get('ARXIV_SORT_BY')
    sort_order = os.environ.get('ARXIV_SORT_ORDER')
    bucket = os.environ.get('ARXIV_BUCKET')
    filename = os.environ.get('ARXIV_FILENAME')

    data = get_arxiv_json_data(
        endpoint,
        query,
        start,
        max_results,
        sort_by,
        sort_order
    )

    aws_s3_writer(
        data,
        bucket,
        filename
    )
