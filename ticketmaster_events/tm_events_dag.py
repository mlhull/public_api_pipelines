#grab airflow dependencies
from airflow.models import Variable
from airflow.decorators import dag
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

#grab etl python callable + lib to export to parquet
from include.tm_etl_step import *
import pyarrow

#extract values from aws for s3 write ou
aws_conn = BaseHook.get_connection('AWSConnection')
aws_key = aws_conn.login
aws_secret = aws_conn.password

api_conn = BaseHook.get_connection('ticketmaster_api_key')
extra = api_conn.extra_dejson
api_token = extra.get('token')

#in airflow ui, set up env vars. call here
s3_url = Variable.get('api_output_tbls')
api_url = Variable.get('ticketmaster_endpoint')

#api query params
dmaId = 264 #denver
classificationName = 'music'  #music
today_date = datetime.utcnow() #grab today's date
date_only = today_date.date() #this will be used for the export dataset
date_30_days_out = today_date + timedelta(days=30) #look 30 days out
startDateTime = today_date.strftime("%Y-%m-%dT%H:%M:%SZ") #filter with a start date before this date
endDateTime= date_30_days_out.strftime("%Y-%m-%dT%H:%M:%SZ") #filter with a start date after this date

##api url and search criteria
url = (f'{api_url}/events.json?classificationName={classificationName}'
    f'&dmaId={dmaId}&apikey={api_token}'
    f'&startDateTime={startDateTime}'
    f'&endDateTime={endDateTime}')

#task function
def process_tm_data():
    
    data = api_call(url) #call api

    if data is not None: #do if api call is good

        events_df = tm_event_to_df(data)

        events_df.to_parquet(
            f'{s3_url}/tm_events_{date_only}.parquet.gzip',  
            compression='gzip',  
            engine='pyarrow',
            storage_options={
                'key': aws_key,  
                'secret': aws_secret 
            }  
        )
    
    #don't generate any message if this fails. we already log the error upstream
    else:
        None

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

def tm_events_pipeline():
    etl_step = PythonOperator(
        task_id='etl_tm_events',
        python_callable=process_tm_data
    )

    etl_step

tm_events_pipeline()
