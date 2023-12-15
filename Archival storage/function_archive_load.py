# ARCHIVE LOGIC-1

# Move and Store the RAW data by year/month/day

#Scheduled for everyday after the dataflow job


import functions_framework
from google.cloud import storage
import logging
import datetime

@functions_framework.http
def archive_raw_data(request):
    try:
        source_bucket_name = 'currencybkt'
        destination_bucket_name = 'archive_new'
        source_filename = 'rate_data.json'
        destination_blob_name = 'forex_of_the_day'

        source_storage_client = storage.Client()
        destination_storage_client = storage.Client()

        current_date = datetime.datetime.now()
        year = str(current_date.year)
        month = str(current_date.month).zfill(2)
        day = str(current_date.day).zfill(2)

        partition_data = f"year={year}/month={month}/day={day}"
        destination_blob_path = f"{partition_data}/{destination_blob_name}.json"

        source_bucket = source_storage_client.bucket(source_bucket_name)
        destination_bucket = destination_storage_client.bucket(destination_bucket_name)

        source_blob = source_bucket.blob(source_filename)
        destination_blob = destination_bucket.blob(destination_blob_path)

        destination_blob.rewrite(source_blob)

        logging.info(f"File {source_filename} update successfully moved to {destination_blob_path}")
        
        return "OK"
    
    except Exception as e:
        logging.error(f"Error moving file from source to destination: {str(e)}")
        return "Error in execution"
