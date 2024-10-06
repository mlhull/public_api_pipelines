import requests #make api calls
import pandas as pd #for data manipulation
from requests.adapters import HTTPAdapter #to mount http session with retry rules
from urllib3.util.retry import Retry #to set up retry rules for http session
import logging #to capture errors

##udf for request session with retry object (incl rules) attached to it. This includes only running if there are server side errors
def retry_session(retries = 3, backoff_factor = 1, status_forcelist = (500, 502, 503, 504)):
    
    #create session request object
    session = requests.Session()

    #pull in retry rules
    retry = Retry(
        total = retries,
        backoff_factor = backoff_factor,
        status_forcelist = status_forcelist
    )

    #create a adapter with the retry rules to mount to the URL session
    adapter = HTTPAdapter(max_retries = retry)

    #mount the adapter to the http session
    session.mount('http://', adapter)

    #return the session as an object
    return session

##udf to call api or - based on status code - trigger retries and/or print error
def api_call(url):
    
    #pull in session object that includes retry rules
    session = retry_session()
    
    try:
        response = session.get(url) #use reqeust session with retry rules
        response.raise_for_status()  #raises bad http error codes
        data = response.json() #returns data as json if valid call made
        return data
    
    except requests.exceptions.RequestException as e:
        logging.exception(f"API request failed: {e}") #generate logging error message to help clarify issue
        return None

#udf to trigger this code only if api_call doesn't error
def tm_event_to_df(data):
    events_data = []

    #for every entity in the data under _embedded -> events
    for event in data['_embedded']['events']:
        event_name = event['name']
        venue_name = event['_embedded']['venues'][0]['name']  
        local_date = event['dates']['start']['localDate']

        #create dict for event
        events_data.append({
            'Event Name': event_name,
            'Venue Name': venue_name,
            'Date': local_date
        })

    #convert dict to df
    #drop dups since these rep records for same event but with diff purchase packages
    events_df = pd.DataFrame(events_data) \
        .drop_duplicates()    

    return events_df
