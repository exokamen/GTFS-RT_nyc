import datetime
import logging
import azure.functions as func
from azure.storage.blob import ContainerClient

from google.transit import gtfs_realtime_pb2
import urllib.request
import urllib
import requests
import pprint
import json
import io
import pandas as pd
#from datetime import datetime


# pylint: disable=unsubscriptable-object  ## this tells pylint to stop being stupid. 
# pylint: disable=no-member
def main(mytimer: func.TimerRequest, outputblob:  func.Out[str]) -> None:

    ## Information about the GTFS-RT URL look-up table. 
    conn_str = "DefaultEndpointsProtocol=https;AccountName=gtfsrttripupdatecollecto;AccountKey=im2OmwPNVb5nxSbo00MbLC+hEqolgkq4wAm39+6zND/Hou3MN6I64pQzgSbiV+2b8QG4LGsjadyVI/i5h+gbnQ==;EndpointSuffix=core.windows.net"
    container = "gtfs-rt-inputs"
    blob_name = "gtfs-rt-url-nyc.csv"

    container_client = ContainerClient.from_connection_string(
        conn_str=conn_str, 
        container_name=container
        )
    ## Download blob as StorageStreamDownloader object (stored in memory). 
    print("Trip Update collector-NYC: Downloading the URLs to get")
    try: 
        downloaded_blob = container_client.download_blob(blob_name)
        providers_df = pd.read_csv(io.StringIO(downloaded_blob.content_as_text()), dtype={'api_header_names': str, 'api_header_keys': str}, na_filter= False)
    except: 
        print('No such file: Input has no URLs!!!')

    ## Filter the URLs that we want (temporary for testing purposes.) When done, remove filter (or set to always true)
    #usage_list = ['SF', 'BA'] #, 'AC', 'CT', 'CC', 'SM', 'ST', 'SO', 'SC']
    #filter1 = (providers_df['Id'].isin(usage_list))
    filter1 = (providers_df['script group'] == providers_df['script group'])
    url_strings = providers_df[filter1]['URL'].values
    pto_names = providers_df[filter1]['Id'].values
    api_header_names = providers_df[filter1]['api_header_name'].values
    api_header_keys = providers_df[filter1]['api_header_key'].values

    ## Define the output dataframe: 
    df = pd.DataFrame()

    ## for each URL in the list that we care about, get the GTFS-RT trip updates: 
    print("Trip Update collector-NYC: Accessing {} real-time feeds.".format(len(pto_names)))
    for url_string, pto_name, api_header_name, api_header_key in zip(url_strings, pto_names, api_header_names, api_header_keys): 
        print("Trip Update collector-NYC: collecting {}...".format(pto_name))
        print(api_header_name, api_header_key)
        df1 = get_rt_Gtfs_TripUpdates(url_string, api_header_name, api_header_key)
        if len(df1) > 0: 
            df1 = df1.sort_values(by='trip_id')
            #df = df1.drop_duplicates()
            df1['operator name'] = pto_name
            df = df.append(df1).drop_duplicates(keep='first')
            logging.info('collector-NYC: Appended data for {}.'.format(pto_name))
        else: 
            logging.info('collector-NYC: {} feed is empty (or broken).'.format(pto_name))
            df = df

    #print('\nCSV String Values:\n', csv_data) 
    if len(df) > 0:  
        outputblob.set(df.to_csv(index=False))
    else: 
        outputblob.set('')

def get_stop_states(entity, timestamp):
    """returns all stop time updates as a df to be appended to the entire one
    """
    stop_time_updates = pd.DataFrame()
    for stu in entity.trip_update.stop_time_update: 
        entity_id       = entity.id
        trip_id         = entity.trip_update.trip.trip_id
        sched_relship   = entity.trip_update.trip.schedule_relationship
        arrival_delay   = stu.arrival.delay
        arrival_time    = stu.arrival.time
        arrival_uncert  = stu.arrival.uncertainty
        departure_delay = stu.departure.delay
        departure_time  = stu.departure.time
        depart_uncert   = stu.departure.uncertainty
        stop_id         = stu.stop_id
        route_id        = entity.trip_update.trip.route_id
        stop_time_updates = stop_time_updates.append(
            {   'server_timestamp':datetime.datetime.fromtimestamp(timestamp), 
                'entity_id':str(entity_id), 
                'trip_id':str(trip_id),
                'route_id': str(route_id),
                'arrival_delay':int(arrival_delay), 
                'arrival_uncertainty': int(arrival_uncert),
                'arrival_time':datetime.datetime.fromtimestamp(arrival_time),
                'departure_delay':int(departure_delay), 
                'departure_time':datetime.datetime.fromtimestamp(departure_time),
                'departure_uncertainty': int(depart_uncert),
                'stop_id':str(stop_id), 
                'schedule_relationsip': str(sched_relship)
            }, ignore_index=True)

    return stop_time_updates

def get_rt_Gtfs_TripUpdates(url, api_header_name, api_header_key):
    """ Access the GTFS-RT at the url and 
    """
    tripupdate = pd.DataFrame()
    #try: 
    feed = gtfs_realtime_pb2.FeedMessage()
    if len(api_header_key) > 0: 
        headers = {api_header_name: api_header_key}
        print('Trip Update collector-NYC: Access key={}'.format(list(headers.values())[0]))
        response = requests.get(url, allow_redirects=True, headers=headers, timeout=15)

    else: 
        print('Trip Update collector-NYC: No access key needed')
        response = requests.get(url, allow_redirects=True, timeout=15)
    
    #print('Trip Update collector-NYC: Access key={}'.format(headers.values[0]))

    feed.ParseFromString(response.content)
    if feed.HasField('header'): 
        timestamp = feed.header.timestamp
    else: 
        logging.info('collector-NYC: Issues with {}! Check for access!'.format(url))
    
    if len(feed.entity) > 0: 
        for entity in feed.entity:
            tripupdate = tripupdate.append(get_stop_states(entity, timestamp))        
    #except: 
    #    logging.info('collector-NYC: ERROR: Cannot access {}!!!'.format(url))
    
    return tripupdate
 
