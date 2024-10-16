#grab airflow dependencies
from airflow.models import Variable
from airflow.decorators import dag
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

#for config file
import configparser

#grab etl python callable + lib to export to parquet
from include.api_etl_module import api_call,tm_transform_and_export

#get aws and ticketmaster connections entered into Airflow UI
aws_conn = BaseHook.get_connection('AWSConnection')
aws_key = aws_conn.login
aws_secret = aws_conn.password

api_conn = BaseHook.get_connection('ticketmaster_api_key')
extra = api_conn.extra_dejson
api_token = extra.get('token')

#grab config file
config = configparser.ConfigParser()
config.read(Variable.get('config'))

#api query params
dmaId = config.getint('ticketmaster_api', 'dmaId')
classificationName = config.get('ticketmaster_api', 'classificationName')
url = config.get('ticketmaster_api', 'ticketmaster_endpoint')
file_name = config.get('ticketmaster_api', 'file_name')

#define s3 buckets
s3_stage = config.get('s3_buckets', 'api_stage_tbls')
s3_results = config.get('s3_buckets', 'api_results_tbls')

#date params
today_date = datetime.utcnow() #grab today's date
date_only = today_date.date() #this will be used for the export dataset
date_30_days_out = today_date + timedelta(days=30) #look 30 days out
startDateTime = today_date.strftime("%Y-%m-%dT%H:%M:%SZ") #filter with a start date before this date
endDateTime= date_30_days_out.strftime("%Y-%m-%dT%H:%M:%SZ") #filter with a start date after this date

##api url and search criteria
url = (f'{url}/events.json?classificationName={classificationName}'
    f'&dmaId={dmaId}&apikey={api_token}'
    f'&startDateTime={startDateTime}'
    f'&endDateTime={endDateTime}')

#task function
@dag(
    start_date=datetime(2024,10,6),
    schedule=None,
    catchup=False,
    tags=['ticketmaster_events'],
    default_args={
        'owner': '[NAME]',
        'retries': 0
    }
)

##api call to stage, or fail. includes retry should server side issue arise
def tm_events_pipeline():
    call_to_stage = PythonOperator(
        task_id='call_to_stage',
        python_callable=api_call,
        op_kwargs={
            'url': url,
            's3_stage': s3_stage,
            'date_only': str(date_only),
            'aws_key': aws_key,
            'aws_secret': aws_secret,
            'file_name': file_name
        }
    )

##load and process data from staging, exporting to results bucket
    process_and_load = PythonOperator(
        task_id='process_and_load',
        python_callable=tm_transform_and_export,
        op_kwargs={
            's3_stage': s3_stage,
            'date_only': str(date_only),
            's3_results': s3_results,
            'aws_key': aws_key,
            'aws_secret': aws_secret,
            'file_name': file_name
        }
    )

    call_to_stage >> process_and_load

tm_events_pipeline()
