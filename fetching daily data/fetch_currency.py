import functions_framework
import json
import requests
from datetime import datetime
from google.cloud import storage
from google.auth import compute_engine
from googleapiclient.discovery import build
from google.cloud import secretmanager


base_url_currencies = 'https://api.freecurrencyapi.com/v1/currencies'
base_url_rates = 'https://api.freecurrencyapi.com/v1/latest'

@functions_framework.http
def main(request):
    try:
        def fetch_secret(secret_id):
            try:
                client = secretmanager.SecretManagerServiceClient()
                name = f"projects/952702250749/secrets/{secret_id}/versions/1"  
                response = client.access_secret_version(request={"name": name})
                return response.payload.data.decode("UTF-8")
            except Exception as e:
                # Handle the exception (e.g., log the error)
                print(f"Error fetching secret: {str(e)}")
                return None

        API_KEY = fetch_secret("freecurrencyAPI-key")

        def fetch_and_store_currency_data():
            try:
                # Fetch currency names
                params = {'apikey': API_KEY}
                response = requests.get(base_url_currencies, params=params)
                name_data = response.json()

                # Store currency names in Cloud Storage
                storage_client = storage.Client()
                bucket = storage_client.get_bucket('currencybkt')
                blob = bucket.blob('name_data.json')
                blob.upload_from_string(data=json.dumps(name_data))

                # Fetch currency rates
                params = {'apikey': API_KEY, 'base_currency': 'INR'}  
                response = requests.get(base_url_rates, params=params)
                rate_data = response.json()
                last_updated_at = response.headers.get('Date')
            
                currency_data = rate_data.get('data', {})
                response_data = {
                    "meta": {"last_updated_at":  last_updated_at},
                    "data": currency_data
                }

                # Store currency rates in Cloud Storage
                blob = bucket.blob('rate_data.json')
                blob.upload_from_string(data=json.dumps(response_data))

                return "Currency data successfully fetched and stored in Cloud Storage."

            except requests.RequestException as e:
                return f"Error with request: {str(e)}"
            except storage.StorageError as e:
                return f"Error storing data in Cloud Storage: {str(e)}"
            except Exception as e:
                return f"An unexpected error occurred: {str(e)}"
        
        result = fetch_and_store_currency_data()
        dataflow_result = run_dataflow_job('unique-atom-406411')

        return f"{result}\n{dataflow_result}"

    except Exception as e:
        return f"Error: {str(e)}"


def run_dataflow_job(project):
    try:
        credentials = compute_engine.Credentials()
        dataflow = build('dataflow', 'v1b3', credentials=credentials)

        template_path = "gs://currencybkt/pipeline-templatenew"
        job_name = "job3-daily" 

        parameters = {
           'job_name': job_name,
           'runner': 'DataflowRunner',
           'project': 'unique-atom-406411',
           'template_location': 'gs://currencybkt/temp'  
        }

        response = dataflow.projects().locations().templates().launch(
            projectId=project,
            gcsPath=template_path,
            location='us-central1',
            body={
                'jobName': job_name,
                'parameters': parameters
            }
        ).execute()
        
        return json.dumps({'status': 'Job triggered successfully', 'response': response})
    
    except Exception as e:
        return json.dumps({'error': str(e)})