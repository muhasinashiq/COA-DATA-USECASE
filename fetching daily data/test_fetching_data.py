#TESTING CASE APPLIED FOR FETCHING DATA FROM OPEN API USING PYTHON AND CLOUD FUNCTION


import functions_framework
import json
import requests
from google.cloud import storage


API_KEY = 'XXX'  # Replace with Free Currency API key
base_url_currencies = 'https://api.freecurrencyapi.com/v1/currencies'
base_url_rates = 'https://api.freecurrencyapi.com/v1/latest'


def fetch_and_store_currency_data(API_KEY, base_url_currencies, base_url_rates):
   try:
       # Fetch currency names
       params = {
           'apikey': API_KEY,
       }
       response = requests.get(base_url_currencies, params=params)
       testname_data = response.json()


       # Store currency names in Cloud Storage
       storage_client = storage.Client()
       bucket = storage_client.get_bucket('currencybkt')
       blob = bucket.blob('testname_data.json')
       blob.upload_from_string(data=json.dumps(testname_data))


       # Fetch currency rates
       params = {
           'apikey': API_KEY,
           'base_currency': 'INR'  # desired base currency
       }
       response = requests.get(base_url_rates, params=params)
       testrate_data = response.json()


       # Store currency rates in Cloud Storage
       bucket = storage_client.get_bucket('currencybkt')
       blob = bucket.blob('testrate_data.json')
       blob.upload_from_string(data=json.dumps(testrate_data))


       return "Currency data successfully fetched and stored in Cloud Storage."


   except Exception as e:
       return f"Error fetching and storing currency data: {str(e)}"

def test_fetch_and_store_currency_data():
    try:
     
        result = fetch_and_store_currency_data(API_KEY, base_url_currencies, base_url_rates)

        # Assert the result or perform checks
        assert isinstance(result, str)  # Example assertion

        # Print test results
        print("Test: fetch_and_store_currency_data - PASSED")
        return True

    except Exception as e:
        print(f"Test: fetch_and_store_currency_data - FAILED with error: {str(e)}")
        return False

@functions_framework.http
def main(request):
    if not test_fetch_and_store_currency_data():
        return "Test failed! Function not executed."
    
    result = fetch_and_store_currency_data(API_KEY, base_url_currencies, base_url_rates)
    
    # Return the result
    return result

