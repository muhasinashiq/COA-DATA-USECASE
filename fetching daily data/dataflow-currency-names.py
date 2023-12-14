import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
import json


import os


# Inorder to set GOOGLE_APPLICATION_CREDENTIALS environment variable in Python code to the path key.json file
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/muhasinashiq1811/df.json"




project_id = 'unique-atom-406411'
bucket_name = 'currencybkt'
data_path = f'gs://{bucket_name}/name_data.json' 
output_table = f'{project_id}.currency.name_data'  


schema ={
  "fields": [
     {"name": "currency_code", "type": "STRING"},
     {"name": "symbol", "type": "STRING"},
     {"name": "name", "type": "STRING"},
     {"name": "symbol_native", "type": "STRING"},
     {"name": "decimal_digits", "type": "INTEGER"},
     {"name": "rounding", "type": "INTEGER"},
     {"name": "code", "type": "STRING"},
     {"name": "name_plural", "type": "STRING"}
  ]
}


options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = project_id
google_cloud_options.job_name = 'job6' 
google_cloud_options.staging_location = f'gs://{bucket_name}/staging'
google_cloud_options.temp_location = f'gs://{bucket_name}/temp'
google_cloud_options.region = 'us-central1'
options.view_as(StandardOptions).runner = 'DataflowRunner'




def parse_json(json_str):
   data = json.loads(json_str)
   parsed_data = []
   for currency_code, currency_data in data['data'].items():
       parsed_data.append({
           'currency_code': currency_code,
           'symbol': currency_data['symbol'],
           'name': currency_data['name'],
           'symbol_native': currency_data['symbol_native'],
           'decimal_digits': currency_data['decimal_digits'],
           'rounding': currency_data['rounding'],
           'code': currency_data['code'],
           'name_plural': currency_data['name_plural']
       })
   return parsed_data




def run():
  with beam.Pipeline(options=options) as pipeline:
      # Read data from Cloud Storage
      data = (
          pipeline
          | 'ReadData' >> beam.io.ReadFromText(data_path)
          | 'ParseJSON' >> beam.Map(parse_json)
          | 'FlattenDict' >> beam.FlatMap(lambda x: x)  # Flatten the list of dictionaries
      )




      # Write data to BigQuery with schema specified
      data | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
          output_table,
          schema=schema,
          write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
          create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
      )




if __name__ == '__main__':
  run()
