# ARCHIVE LOGIC-2

#Appending Daily raw data in the same file(copying)

import functions_framework
from google.cloud import storage
import logging
import datetime

@functions_framework.http
def move_to_archive(request):
    try:
        source_bucket_name = 'currencybkt'
        destination_bucket_name = 'archive_bkt'
        source_file_name = 'rate_data.json'

        current_date = datetime.datetime.now()
        year = str(current_date.year)
        month = str(current_date.month).zfill(2)

        partition_data = f"year={year}/month={month}"
        destination_file_name = 'forex_archive.json'

        storage_client = storage.Client()
        source_bucket = storage_client.get_bucket(source_bucket_name)
        destination_bucket = storage_client.get_bucket(destination_bucket_name)

        source_blob = source_bucket.blob(source_file_name)
        destination_blob = destination_bucket.blob(f"{partition_data}/{destination_file_name}")

        # Read existing content from the destination file
        existing_content = destination_blob.download_as_string() if destination_blob.exists() else b''
        file_content = source_blob.download_as_string()

        # Append new data to existing content
        updated_content = existing_content + b"\n" + file_content

        # Upload the updated content to the destination file
        destination_blob.upload_from_string(updated_content)

        logging.info(f"Daily data appended to {destination_blob.name}")

        return "OK, data appended to archival file."

    except Exception as e:
        logging.error(f"Error moving data: {e}")
        return "Error occurred"