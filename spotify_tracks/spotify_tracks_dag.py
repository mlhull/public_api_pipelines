#grab airflow dependencies
from airflow.decorators import dag
from airflow.providers.http.operators.http import SimpleHttpOperator  
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.models import Variable #to locate config file
import configparser #to read in config file
from airflow.hooks.base import BaseHook

#grab etl python callable + lib to export to parquet
from include.api_etl_module import get_client_secret_id, api_call_with_token, s_transform_and_export

#get aws connections entered into Airflow UI
aws_conn = BaseHook.get_connection('AWSConnection')
aws_key = aws_conn.login
aws_secret = aws_conn.password

#grab config file
config = configparser.ConfigParser()
config.read(Variable.get('config'))

#api query params + connection
http_conn_id = config.get('spotify_api', 'http_conn_id')
post_endpoint = config.get('spotify_api', 'post_endpoint') #this is the endpoint for the url featured in the Airflow connection
spotify_api_endpoint = config.get('spotify_api', 'spotify_api_endpoint')#this is the endpoint to do the GET api call
api_query=config.get('spotify_api', 'api_query')

#for file name
file_name = config.get('spotify_api','file_name')
today_date = datetime.utcnow() #grab today's date
date_only = today_date.date() #this will be used for the export dataset

#define s3 buckets
s3_stage = config.get('s3_buckets', 'api_stage_tbls')
s3_results = config.get('s3_buckets', 'api_results_tbls')

##set dag
@dag(
    start_date=datetime(2024,10,9),
    schedule=None,
    catchup=False,
    tags=['spotify_tracks'],
    default_args={
        'owner': '[NAME]',
        'retries': 0
    }
)

def spotify_tracks_pipeline():

    #grab airflow http connection details and pass credential using xcom
    get_connection = PythonOperator(
        task_id='get_client_secret_id',
        python_callable=get_client_secret_id,
        op_kwargs={'http_conn_id': http_conn_id},
        do_xcom_push=True
    )

    #use airflow connection to get token and pass credential using xcom
    get_token_task = SimpleHttpOperator(
        task_id='get_token',
        method='POST',
        log_response=True,
        http_conn_id=http_conn_id, #this gets the base url
        endpoint=post_endpoint, #applied on top of base url
        data=(
        'grant_type=client_credentials&'
        'client_id={{ ti.xcom_pull(task_ids="get_client_secret_id", key="client_id") }}&'
        'client_secret={{ ti.xcom_pull(task_ids="get_client_secret_id", key="client_secret") }}'
        ),  #set as a data string to meet expected format
        headers={'Content-Type': 'application/x-www-form-urlencoded'}, #designate expected format
        response_filter=lambda response: response.json().get('access_token'), #parses response to pass along to next task
        do_xcom_push=True
    )

    #makes call using xcom token and loads json data into staging s3 bucket
    call_to_stage = PythonOperator(
        task_id='call_to_stage',
        python_callable=api_call_with_token,
        op_kwargs={
            'url': f'{spotify_api_endpoint}{api_query}',
            's3_stage': s3_stage,
            'date_only': str(date_only),
            'aws_key': aws_key,
            'aws_secret': aws_secret,
            'file_name': file_name,
            'params': None,
            'auth_token_header': '{{ task_instance.xcom_pull(task_ids="get_token") }}'  #grab auth token from previous task using xcom
        }
    )

    #grabs data from staging bucket, transforms it and loads it to results bucket
    transform_and_export_task = PythonOperator(
        task_id='process_and_load',
        python_callable=s_transform_and_export,
        op_kwargs={
            's3_stage': s3_stage,
            'date_only': str(date_only),
            's3_results': s3_results,
            'aws_key': aws_key,
            'aws_secret': aws_secret,
            'file_name': file_name
        }
    )

    # Update the task dependencies
    get_connection >> get_token_task >> call_to_stage >> transform_and_export_task

spotify_tracks_pipeline()

