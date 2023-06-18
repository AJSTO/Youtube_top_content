import os
import uuid
import re
from datetime import datetime, timedelta, date
from dateutil import parser

import pandas as pd
import pandas_gbq

from google.cloud import bigquery
from google.oauth2 import service_account
from google.oauth2 import credentials
import google.oauth2.credentials
import google_auth_oauthlib.flow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from googleapiclient.discovery import build

import yaml

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteTableOperator,
)


from youtube_dag.schemas import (
    CHANNEL_INFO_SCHEMA,
    CHANNEL_INFO_CLUSTERING,
    CATEGORIES_NAME_SCHEMA,
    CHANNEL_CATEGORIES_SCHEMA,
    CHANNEL_CATEGORIES_CLUSTERING,
    DAILY_TOP_VIDEOS_SCHEMA,
    DAILY_TOP_VIDEOS_CLUSTERING,
)
from youtube_dag.methods import convert_duration_to_seconds

default_args = {
    'owner': 'adam',
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
}

with open('dags/youtube_dag/config.yaml', 'r') as f:
    config = yaml.safe_load(f)

PROJECT_ID = config['PROJECT_ID']
DATASET_NAME = config['DATASET_NAME']
TABLE_CHANNEL_INFO = config['TABLE_CHANNEL_INFO']
TABLE_CHANNEL_CATEGORIES = config['TABLE_CHANNEL_CATEGORIES']
TABLE_CATEGORIES_NAME = config['TABLE_CATEGORIES_NAME']
TABLE_DAILY_TOP_VIDEOS = config['TABLE_DAILY_TOP_VIDEOS']
TEMP_TABLE_CHANNEL_X_CATEGORIES = config['TEMP_TABLE_CHANNEL_X_CATEGORIES']
JSON_KEY_BQ = config['JSON_KEY_BQ']
KEY_PATH = f"dags/youtube_dag/{JSON_KEY_BQ}"

CREDENTIALS_BQ = service_account.Credentials.from_service_account_file(
    KEY_PATH,
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

CLIENT_BQ = bigquery.Client(credentials=CREDENTIALS_BQ, project=CREDENTIALS_BQ.project_id)

CREDENTIALS_YT = service_account.Credentials.from_service_account_file(
    KEY_PATH,
    scopes=["https://www.googleapis.com/auth/youtube.readonly"]
)
CLIENT_YT = build("youtube", "v3", credentials=CREDENTIALS_YT)

REGION_CODE = 'PL'  # Specify the region for which you want to retrieve the categories
NUM_OF_TOP_VIDEOS_TO_RECEIVE = 100


def getting_categories(ti):
    """
    Airflow task to retrieve and store video categories in BigQuery.

    Args:
        ti (airflow.models.TaskInstance): The TaskInstance object.

    Returns:
        None
    """
    is_first_run = ti.xcom_pull(task_ids='creating_tables.create_table_categories_name', key='bigquery_table')
    if is_first_run:
        # Request the list of video categories
        categories_response = CLIENT_YT.videoCategories().list(
            part='snippet',
            regionCode=REGION_CODE
        ).execute()

        # Process the response and extract the category IDs and names
        categories = []
        for category in categories_response['items']:
            category_id = category['id']
            category_name = category['snippet']['title']
            categories.append((category_id, category_name))
        categories_name_df = pd.DataFrame(categories, columns=['category_id', 'category_name'])
        pandas_gbq.to_gbq(
            categories_name_df,
            f'{DATASET_NAME}.{TABLE_CATEGORIES_NAME}',
            project_id=f'{PROJECT_ID}',
            if_exists='append',
            credentials=CREDENTIALS_BQ,
        )


def get_top_videos_daily(ti):
    """
    Airflow task to retrieve and store daily top videos in BigQuery.

    Args:
        ti (airflow.models.TaskInstance): The TaskInstance object.

    Returns:
        None
    """
    request = CLIENT_YT.videos().list(
        part="snippet,contentDetails,statistics",
        chart="mostPopular",
        regionCode=REGION_CODE,
        maxResults=NUM_OF_TOP_VIDEOS_TO_RECEIVE
    )
    response = request.execute()
    items = response["items"]
    video_data = []
    for item in items:
        video_id = item["id"]
        channel_id = item["snippet"]["channelId"]
        video_category_id = item["snippet"]["categoryId"]
        video_title = item["snippet"]["title"]
        video_description = item["snippet"]["description"]
        video_published = datetime.fromisoformat(item["snippet"]["publishedAt"][:-1])
        video_duration = convert_duration_to_seconds(item["contentDetails"]["duration"])
        video_views = int(item["statistics"]["viewCount"])
        video_likes = int(item["statistics"].get("likeCount", 0))
        video_comments = int(item["statistics"].get("commentCount", 0))

        video_data.append({
            "video_id": video_id,
            "channel_id": channel_id,
            "video_category_id": video_category_id,
            "video_title": video_title,
            "video_description": video_description,
            "video_published": video_published,
            "video_duration": video_duration,
            "video_views": video_views,
            "video_likes": video_likes,
            "video_comments": video_comments,
            "video_captured_at": pd.Timestamp.now().date()
        })

    top_videos_df = pd.DataFrame(video_data)
    pandas_gbq.to_gbq(
        top_videos_df,
        f'{DATASET_NAME}.{TABLE_DAILY_TOP_VIDEOS}',
        project_id=f'{PROJECT_ID}',
        if_exists='append',
        credentials=CREDENTIALS_BQ,
    )
    channel_ids_set = set(top_videos_df.channel_id)
    ti.xcom_push(key='channel_ids', value=channel_ids_set)
    channel_with_categories_dict = top_videos_df[['channel_id', 'video_category_id']].to_dict('records')
    ti.xcom_push(key='channel_with_categories_df', value=channel_with_categories_dict)


def get_channel_info(ti):
    """
    Airflow task to retrieve channel information and store it in BigQuery.

    Args:
        ti (airflow.models.TaskInstance): The TaskInstance object.

    Returns:
        None
    """
    channels_id = set(ti.xcom_pull(task_ids='get_top_videos_daily', key='channel_ids'))
    query = f"""
        SELECT DISTINCT
            channel_id
        FROM 
            `{PROJECT_ID}.{DATASET_NAME}.{TABLE_CHANNEL_INFO}`
        ;
        """
    channel_ids_from_bq = CLIENT_BQ.query(query)
    channel_id_set = set()
    for row in channel_ids_from_bq:
        channel_id = row['channel_id']
        channel_id_set.add(channel_id)
    channels_id = channels_id.union(channel_id_set)
    ti.xcom_push(key='all_collected_channel_ids', value=channels_id)
    channels_data = []
    for id in channels_id:
        request = CLIENT_YT.channels().list(
            part="snippet,statistics",
            id=id
        )
        response = request.execute()
        channel_info = response["items"][0]
        channel_name = channel_info['snippet']['title']
        channel_id = channel_info['id']
        kind = channel_info['kind']
        channel_published = parser.isoparse(channel_info['snippet']['publishedAt'])
        channel_logo_url = channel_info['snippet']['thumbnails']['medium']['url']
        total_views = int(channel_info['statistics']['viewCount'])
        channel_market = channel_info['snippet'].get("country", 'None')
        channel_subs = int(channel_info['statistics']['subscriberCount'])
        channel_videos = int(channel_info['statistics']['videoCount'])
        channel_description = channel_info['snippet']['description']

        channels_data.append({
            "channel_id": channel_id,
            "channel_name": channel_name,
            "kind": kind,
            "channel_published": channel_published,
            "channel_logo_url": channel_logo_url,
            "total_views": total_views,
            "channel_market": channel_market,
            "channel_subs": channel_subs,
            "channel_videos": channel_videos,
            "channel_description": channel_description,
            "updated_at": pd.Timestamp.now().date()
        })
    ti.xcom_push(key='channels_data', value=channels_data)
    pandas_gbq.to_gbq(
        pd.DataFrame(channels_data),
        f'{DATASET_NAME}.{TABLE_CHANNEL_INFO}',
        project_id=f'{PROJECT_ID}',
        if_exists='append',
        credentials=CREDENTIALS_BQ,
    )


def updating_channels_categories(ti):
    """
    Airflow task to update the channel categories in BigQuery.

    Args:
        ti (airflow.models.TaskInstance): The TaskInstance object.

    Returns:
        None
    """
    channel_categories = pd.DataFrame(
        ti.xcom_pull(task_ids='get_top_videos_daily', key='channel_with_categories_df')
    )
    # Define the temporary table name
    temp_table_name = f"{DATASET_NAME}.{TEMP_TABLE_CHANNEL_X_CATEGORIES}"

    # Define the DataFrame with the new values
    df_new_values = channel_categories

    # Write the DataFrame to the temporary table
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job = CLIENT_BQ.load_table_from_dataframe(df_new_values, temp_table_name, job_config=job_config)
    job.result()

    # Insert the non-repeating pairs from the temporary table to the target table
    query = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_NAME}.{TABLE_CHANNEL_CATEGORIES}` (channel_id, video_category_id)
        SELECT DISTINCT t.channel_id, t.video_category_id
        FROM `{temp_table_name}` AS t
        LEFT JOIN `{PROJECT_ID}.{DATASET_NAME}.{TABLE_CHANNEL_CATEGORIES}` AS existing
        ON t.channel_id = existing.channel_id AND t.video_category_id = existing.video_category_id
        WHERE existing.channel_id IS NULL AND existing.video_category_id IS NULL
    """

    job = CLIENT_BQ.query(query)
    job.result()


def last_channel_activity(ti):
    """
    Airflow task to process the last channel activity.

    Args:
        ti (airflow.models.TaskInstance): The TaskInstance object.

    Returns:
        None
    """
    channels_id = list(ti.xcom_pull(task_ids='get_channel_info', key='all_collected_channel_ids'))

    # TO BE DEVELOPED


with DAG(
        default_args=default_args,
        dag_id='Youtube_top_content_V2',
        description='Capturing top content on youtube',
        start_date=datetime(2023, 6, 9),
        schedule_interval='@daily',
        catchup=False,
) as dag:
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_dataset',
        project_id=PROJECT_ID,
        dataset_id=DATASET_NAME
    )
    with TaskGroup(group_id='creating_tables') as creating_tables:
        create_table_channel_info = BigQueryCreateEmptyTableOperator(
            task_id='create_table_channel_info',
            project_id=PROJECT_ID,
            dataset_id=DATASET_NAME,
            table_id=TABLE_CHANNEL_INFO,
            schema_fields=CHANNEL_INFO_SCHEMA,
            cluster_fields=CHANNEL_INFO_CLUSTERING,
        )
        create_table_categories_name = BigQueryCreateEmptyTableOperator(
            task_id='create_table_categories_name',
            project_id=PROJECT_ID,
            dataset_id=DATASET_NAME,
            table_id=TABLE_CATEGORIES_NAME,
            schema_fields=CATEGORIES_NAME_SCHEMA,
        )
        create_table_channel_categories = BigQueryCreateEmptyTableOperator(
            task_id='create_table_channel_categories',
            project_id=PROJECT_ID,
            dataset_id=DATASET_NAME,
            table_id=TABLE_CHANNEL_CATEGORIES,
            schema_fields=CHANNEL_CATEGORIES_SCHEMA,
            cluster_fields=CHANNEL_CATEGORIES_CLUSTERING,
        )
        create_table_daily_top_videos = BigQueryCreateEmptyTableOperator(
            task_id='create_table_daily_top_videos',
            project_id=PROJECT_ID,
            dataset_id=DATASET_NAME,
            table_id=TABLE_DAILY_TOP_VIDEOS,
            schema_fields=DAILY_TOP_VIDEOS_SCHEMA,
            cluster_fields=DAILY_TOP_VIDEOS_CLUSTERING,
        )
        [create_table_channel_info, create_table_categories_name, create_table_channel_categories,
         create_table_daily_top_videos]
    getting_categories = PythonOperator(
        task_id='getting_categories',
        python_callable=getting_categories,
    )
    get_top_videos_daily = PythonOperator(
        task_id='get_top_videos_daily',
        python_callable=get_top_videos_daily,
    )
    get_channel_info = PythonOperator(
        task_id='get_channel_info',
        python_callable=get_channel_info,
    )
    updating_channels_categories = PythonOperator(
        task_id='updating_channels_categories',
        python_callable=updating_channels_categories,
    )
    delete_temp_table = BigQueryDeleteTableOperator(
        task_id='delete_temp_table',
        deletion_dataset_table=f"{PROJECT_ID}.{DATASET_NAME}.{TEMP_TABLE_CHANNEL_X_CATEGORIES}",
    )

    create_dataset >> creating_tables >> getting_categories >> get_top_videos_daily >> get_channel_info >> \
        updating_channels_categories >> delete_temp_table
