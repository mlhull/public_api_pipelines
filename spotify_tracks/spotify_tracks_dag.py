#grab airflow dependencies
from airflow.decorators import dag
from airflow.providers.http.operators.http import SimpleHttpOperator  
from datetime import datetime
from airflow.operators.python import PythonOperator
from include.spotify_etl_step import *
import pyarrow

#set api query params
artist_id = '4Z8W4fKeB5YxbusRsdQVPb'
params = {'market': 'US'}

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

    #grab airflow connection details
    get_connection = PythonOperator(
        task_id='get_connection',
        python_callable=get_spotify_credentials,
        do_xcom_push=True
    )

    #use airflow connection to get token
    get_token_task = SimpleHttpOperator(
        task_id='get_token',
        method='POST',
        log_response=True,
        http_conn_id='spotify_auth', #this gets the base url
        endpoint='/api/token', #applied on top of base url
        data=(
        'grant_type=client_credentials&'
        'client_id={{ ti.xcom_pull(task_ids="get_connection", key="client_id") }}&'
        'client_secret={{ ti.xcom_pull(task_ids="get_connection", key="client_secret") }}'
    ),  #set as a data string to meet expected format
        headers={'Content-Type': 'application/x-www-form-urlencoded'}, #designate expected format
        response_filter=lambda response: response.json().get('access_token'), #parses response to pass along to next task
        do_xcom_push=True
    )

    #call api using token
    get_tracks_task = SimpleHttpOperator(
        task_id='get_tracks',
        method='GET',
        log_response=True,
        http_conn_id='spotify_api',
        endpoint= f"/v1/artists/{artist_id}/top-tracks",
        params=params,
        headers={"Authorization": "Bearer {{ ti.xcom_pull(task_ids='get_token', key='return_value') }}"}, #grab auth token
        response_filter=lambda response: response.json(), #parse response as json to pass along to next task
        do_xcom_push=True
    )
    
    #select data of interest and transform data to pd df
    transform_and_export_task = PythonOperator(
        task_id='transform_and_export_tracks_data',
        python_callable=transform_and_export,
        provide_context=True #add context to pass ti (taskinstance)
    )

    # Update the task dependencies
    get_connection >> get_token_task >> get_tracks_task >> transform_and_export_task

spotify_tracks_pipeline()
