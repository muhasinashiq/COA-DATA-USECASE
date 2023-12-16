# TEST CASES IMPLEMENTED FOR CHECKING THE WORKING OF DEFINED FUNCTIONS IN PIPELINE


import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
import json
from datetime import datetime
import uuid
import logging
import unittest

#Dataflow options
project_id = 'unique-atom-406411'
bucket_name = 'currencybkt'
data_path = f'gs://{bucket_name}/rate_data.json'  
output_table = f'{project_id}.test.test1'  

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
google_cloud_options.job_name = 'eg'  
google_cloud_options.staging_location = f'gs://{bucket_name}/staging'
google_cloud_options.temp_location = f'gs://{bucket_name}/temp'
google_cloud_options.region = 'us-central1'
options.view_as(StandardOptions).runner = 'DataflowRunner'

class TestDataflowMethods(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_parse_json_single_currency(self):
        # Test parsing a JSON string with only one currency
        sample_json_str = '{"meta": {"last_updated_at": "Fri, 01 Jan 2023 12:00:00 UTC"}, "data": {"USD": 1.0}}'
        parsed_data = parse_json(sample_json_str)
        
        # Add assertions to validate the parsing logic for a single currency
        self.assertEqual(len(parsed_data), 1)
        self.assertEqual(parsed_data[0]['foreign_currency'], 'USD')
        self.assertAlmostEqual(parsed_data[0]['exchange_rate'], 1.0)

    def test_parse_json_empty_data(self):
        # Test parsing an empty JSON string
        sample_json_str = '{}'
        parsed_data = parse_json(sample_json_str)
        
        # Add assertions to handle empty data scenarios
        self.assertEqual(len(parsed_data), 0)

    def test_round_exchange_rate(self):
        # Test RoundExchangeRateFn function
        sample_element = {'exchange_rate': 1.23456789}
        rounder = RoundExchangeRateFn()
        processed_element = next(rounder.process(sample_element))

        # Add assertions to validate the rounding logic
        self.assertAlmostEqual(processed_element['exchange_rate'], 1.234568, places=6)
       

def run_tests():
    suite = unittest.TestLoader().loadTestsFromTestCase(TestDataflowMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)


def run():
   with beam.Pipeline(options=options) as pipeline:
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
        return [] 

    current_timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    result = []
    base_currency = 'INR'
    for currency, exchange_rate in currency_data.items():
        uuid = f"{currency}/{formatted_date}"
        result.append({'UUID': uuid, 'fetch_timestamp': current_timestamp,'currency_date': formatted_date,'base_currency': base_currency, 'foreign_currency': currency, 'exchange_rate': exchange_rate})

    return result


class RoundExchangeRateFn(beam.DoFn):
    def process(self, element):
        element['exchange_rate'] = round(element['exchange_rate'], 6)  
        yield element
        

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    logging.info("Running unit tests...")
    run_tests()
    logging.info("Unit tests executed.")
    pass