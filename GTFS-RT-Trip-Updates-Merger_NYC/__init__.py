import datetime
import logging

import azure.functions as func
import os, uuid
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
import io
import pandas as pd
# from sqlalchemy import create_engine

# pylint: disable=unsubscriptable-object  ## this tells pylint to stop being stupid. 
# pylint: disable=no-member
def main(dailyTimer: func.TimerRequest, outbputblob:  func.Out[str]) -> None:

    trip_updates_all_df = pd.DataFrame()

    # username = 'kamen'
    # password = 'zN36XJ*@+s>8WE>+'
    # engine = create_engine('postgresql://{0}:{1}@rebel-data.cwmhmg3utxyp.us-west-1.rds.amazonaws.com/postgres'.format(username,password))

    connect_str = 'DefaultEndpointsProtocol=https;AccountName=gtfsrttrpupdnyc;AccountKey=0KoJsx8eJVNn7hvoYFicO5gpetwsTS/RGF2updmjqbGvq80UWEdrOcLzZu7T9ptj61VfcA6JF0msJ8F4SN5/ZA==;EndpointSuffix=core.windows.net'
    try:
        #print("Azure Blob Storage v" + __version__ + " - Python quickstart sample")
        # Create the BlobServiceClient object which will be used to create a container client
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)

        # Create a unique name for the container
        #container_name = str(uuid.uuid4())

        # Create the container client
        container_client = blob_service_client.get_container_client('gtfs-rt')#   create_container(container_name)

        # List the blobs in the container
        blob_list = container_client.list_blobs()
        blob_list = list(blob_list)
        print("Trip Updates: There are {} blobs to go through".format(len(blob_list)))
        for blob in blob_list:
            print("\t" + blob.name)

            ## Download blob as StorageStreamDownloader object (stored in memory) 
            ## and read it as CSV, then append it to the big file. 
            try: 
                downloaded_blob     = container_client.download_blob(blob.name)
                trip_updates_df     = pd.read_csv(io.StringIO(downloaded_blob.content_as_text()))
                trip_updates_all_df = trip_updates_all_df.append(trip_updates_df).drop_duplicates(keep='first')
            except: 
                #pass
                print('No such file to download! But WHY??')

            try: 
                # Delete container after reading it. 
                container_client.delete_blob(blob.name, delete_snapshots='include')
            except: 
                print('No such file to delete!!')

        ## Output the merged data to another blob
        if len(trip_updates_all_df) > 0:  
            print("Trip Updates: Writing output")
            outbputblob.set(trip_updates_all_df.to_csv(index=False))
            #trip_updates_all_df.to_sql('gtfs_rt_trip_updates', engine, if_exists='append', schema='gtfs_rt', index_label='index', method='multi')
        else: 
            print("Trip Updates: Nothing to write")
            outbputblob.set('')
        print("Trip Updates: Processed {} blobs.".format(len(blob_list)))

        # ## Now ship to SQL
        # if len(trip_updates_all_df) > 0:  
        #     print("Shipping to SQL DB...")
        #     trip_updates_all_df.to_sql('gtfs_rt_trip_updates', engine, if_exists='append', schema='gtfs_rt', index_label='index', method='multi')
        #     print("Done!")

    except Exception as ex:
        pass
        print('Exception! Something went horribly wrong: ')
        print(ex)

