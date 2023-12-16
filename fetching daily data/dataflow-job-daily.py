import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
import json
from datetime import datetime
import uuid


#Dataflow options
project_id = 'unique-atom-406411'
bucket_name = 'currencybkt'
data_path = f'gs://{bucket_name}/rate_data.json' 
output_table = f'{project_id}.currencyupdated.forex_update_DB' 


#schema for the BigQuery table
schema = {
  'fields': [
      {'name': 'UUID', 'type': 'STRING'},
      {'name': 'fetch_timestamp', 'type': 'TIMESTAMP'},
      {'name': 'currency_date', 'type': 'DATE'},
      {'name': 'base_currency', 'type': 'STRING'},
      {'name': 'foreign_currency', 'type': 'STRING'},
      {'name': 'exchange_rate', 'type': 'FLOAT'},
  ]
}


options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = project_id
google_cloud_options.job_name = 'job-test' 
google_cloud_options.staging_location = f'gs://{bucket_name}/staging'
google_cloud_options.temp_location = f'gs://{bucket_name}/temp'
google_cloud_options.region = 'us-central1'
options.view_as(StandardOptions).runner = 'DataflowRunner'




def run():
  with beam.Pipeline(options=options) as pipeline:
      # Read data from Cloud Storage
      data = (
          pipeline
          | 'ReadData' >> beam.io.ReadFromText(data_path)
          | 'ParseJSON' >> beam.Map(parse_json)
          | 'FlattenDict' >> beam.FlatMap(lambda x: x)  # Flatten the list of dictionaries
          | 'Round Exchange Rate' >> beam.ParDo(RoundExchangeRateFn())
      )


      # Write data to BigQuery with schema specified
      data | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
          output_table,
          schema=schema,
          write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
          create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
      )




def parse_json(json_str):
    import datetime
    import uuid

    def format_date(date_str):
        if not date_str:
            return None
        
        # Convert the date string to a datetime object
        date_time_obj = datetime.datetime.strptime(date_str, '%a, %d %b %Y %H:%M:%S %Z')

        # Format the datetime object to ISO 8601 format
        iso_formatted_date = date_time_obj.strftime('%Y-%m-%d')

        return iso_formatted_date

    data = json.loads(json_str)
    meta_info = data.get('meta', {})
    currency_data = data.get('data', {})
    last_updated_at = meta_info.get('last_updated_at', '')  # Extract date from 'last_updated_at'
    formatted_date = format_date(last_updated_at)
    
    if formatted_date is None:
        return []  # Return an empty list if last_updated_at is empty

    current_timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Extract data and create a list of dictionaries with unique UUIDs for each row
    result = []
    base_currency = 'INR'
    for currency, exchange_rate in currency_data.items():
        uuid = f"{currency}/{formatted_date}"
        result.append({'UUID': uuid, 'fetch_timestamp': current_timestamp,'currency_date': formatted_date,'base_currency': base_currency, 'foreign_currency': currency, 'exchange_rate': exchange_rate})

    return result



# def generateUUID():
#     return str(uuid.uuid4())


class RoundExchangeRateFn(beam.DoFn):
   def process(self, element):
       element['exchange_rate'] = round(element['exchange_rate'], 6)  # Rounding to 6 decimal places
       yield element
      


if __name__ == '__main__':
  run()


