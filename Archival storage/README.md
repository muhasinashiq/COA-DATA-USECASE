# COA-DATA-USECASE-
Archive Currency Data Function

This Python script is designed as a Cloud Function to move daily currency exchange rate data from one Google Cloud Storage bucket to another, organizing the data into monthly partitions.

Setup

Google Cloud Setup

Ensure you have a Google Cloud project configured with necessary permissions to access Cloud Storage.
Modify the source_bucket_name, destination_bucket_name, source_file_name, and other relevant configurations according to your GCP setup.


Python Libraries

Ensure you have installed the required Python libraries:
functions_framework
google-cloud-storage
logging
datetime
Usage

The script defines a Cloud Function move_to_archive triggered by an HTTP request. It performs the following steps:

Retrieves the daily currency data file (rate_data.json) from the source bucket.
Organizes the data into monthly partitions (year/month structure) within the destination bucket (archive_bkt).
Appends the daily data to the respective monthly archive file (forex_archive.json) in the destination bucket.
To trigger the function, use an HTTP request to the provided endpoint URL.

Important Notes:

Bucket Configuration: Ensure correct configuration of source_bucket_name and destination_bucket_name.
Partitioning: The script organizes data into monthly partitions based on the current date (year and month).
Existing Content: The function reads existing content from the destination file, appends new data, and uploads the updated content.
