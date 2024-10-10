#
from datetime import datetime
from airflow.providers.http.hooks.http import HttpHook
from airflow.models import Variable
from airflow.hooks.base import BaseHook
import pandas as pd

##for extract
def get_spotify_credentials(**kwargs): #kwargs parameter lets us access airflow task instance to use xcoms
    #grab connection from airflow ui
    hook = HttpHook(http_conn_id='spotify_auth')

    #get client_id and client_secret
    connection = hook.get_connection(hook.http_conn_id)
    extras = connection.extra_dejson
    client_id = extras.get('client_id')
    client_secret = extras.get('client_secret')
    
    #push xcom to next task
    kwargs['ti'].xcom_push(key='client_id', value=client_id)
    kwargs['ti'].xcom_push(key='client_secret', value=client_secret)

##for transform and load
def transform_and_export(ti):

    #set xcom response to pass values between tasks
    response = ti.xcom_pull(task_ids='get_tracks')

    #create empty list
    track_data = []

    #iterate through track for data points of interest
    for track in response['tracks']:
        artist_name = track['artists'][0]['name']
        album_name = track['album']['name']
        album_release_date = track['album']['release_date']
        track_name = track['name']

        #add data points to list
        track_data.append({
            'Artist Name': artist_name,
            'Album Name': album_name,
            'Album Release Date': album_release_date,
            'Track Name': track_name
        })

    #transform to pandas df
    track_data_df = pd.DataFrame(track_data)

    #in airflow ui, set up env vars. call here
    s3_url = Variable.get('api_output_tbls')

    #extract values from aws for s3 write ou
    aws_conn = BaseHook.get_connection('AWSConnection')
    aws_key = aws_conn.login
    aws_secret = aws_conn.password

    #set output file date
    today_date = datetime.today().date()

    #export as parquet to s3 bucket
    track_data_df.to_parquet(f'{s3_url}/s_track_data_{today_date}.parquet.gzip',  
    compression='gzip',  
    engine='pyarrow',
    storage_options={
        'key': aws_key,  
        'secret': aws_secret} 
    )