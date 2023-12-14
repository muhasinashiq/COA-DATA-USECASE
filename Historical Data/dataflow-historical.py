import apache_beam as beam
import json
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
import uuid


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



# def generateUUID():
#     return str(uuid.uuid4())- IF RANDOM UUID IS REQUIRED



def run():
  options = PipelineOptions()




  google_cloud_options = options.view_as(GoogleCloudOptions)
  google_cloud_options.project = 'unique-atom-406411'
  google_cloud_options.job_name = 'job2'
  google_cloud_options.staging_location = 'gs://currencybkt/staging'
  google_cloud_options.temp_location = 'gs://currencybkt/temp'
  google_cloud_options.region = 'us-central1'
  options.view_as(StandardOptions).runner = 'DataflowRunner'




  with beam.Pipeline(options=options) as pipeline:
      parsed_data = (
          pipeline
          | 'Read JSON' >> beam.io.ReadFromText('gs://currencybkt/hist_edited (2).json')
          | 'Parse JSON' >> beam.ParDo(ParseJson())
          | 'Round Exchange Rate' >> beam.ParDo(RoundExchangeRateFn())
      
      )




      # Write data to BigQuery with schema specified
      parsed_data | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
          'unique-atom-406411:currencyupdated.forex_update_DB',
          schema=schema,
          write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
          create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
      )




class ParseJson(beam.DoFn):
  def process(self, element):
      import uuid
      import datetime
      data = json.loads(element)
      current_timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
      parsed_data = []
     
      base_currency = 'INR'
      for date, currencies in data['data'].items():
          for currency, exchange_rate in currencies.items():
              # new_uuid = str(uuid.uuid4())
              uuid = f"{currency}/{date}"
              parsed_data.append({
                  'UUID':uuid,
                  'fetch_timestamp': current_timestamp,
                  'currency_date': date,
                  'base_currency': base_currency,
                  'foreign_currency': currency,
                  'exchange_rate': exchange_rate
              })
            
      return parsed_data



class RoundExchangeRateFn(beam.DoFn):
  def process(self, element):
      element['exchange_rate'] = round(element['exchange_rate'], 6)  # Rounding to 6 decimal places
      yield element




if __name__ == '__main__':
  run()
