Currency Data Processing Pipeline
This Apache Beam pipeline retrieves currency exchange rate data from a JSON file stored in Google Cloud Storage, transforms the data, and loads it into a BigQuery table.

Setup
Google Cloud Setup

Ensure you have a Google Cloud project configured with the necessary permissions to access Cloud Storage and BigQuery.
Replace 'unique-atom-406411' with your Google Cloud project ID in the script.
Pipeline Options Configuration

Modify the options like bucket_name, data_path, output_table, and other relevant configurations according to your GCP setup.
Schema for BigQuery Table

Adjust the schema definition in the script (schema variable) to match the structure of your BigQuery table.
Python Libraries

Ensure you have installed the required Python libraries:
apache_beam
json
datetime
uuid
Usage
The script defines an Apache Beam pipeline that performs the following steps:

Reads data from a JSON file stored in Google Cloud Storage.
Parses the JSON data, processes it, and prepares it for insertion into BigQuery.
Rounds the exchange rates to 6 decimal places.
Writes the processed data to a specified BigQuery table.
To execute the pipeline, run the Python script:

bash
Copy code
python script_name.py
Important Notes
Google Cloud Options: Ensure the correct configuration of Google Cloud options like project_id, bucket_name, and output_table.
Schema: Update the schema definition to match the structure of your BigQuery table.
Data Path: Adjust the data_path variable to point to the correct location of your JSON data in Google Cloud Storage.
UUID Generation: The script generates a unique UUID for each row in the BigQuery table based on currency and date.
