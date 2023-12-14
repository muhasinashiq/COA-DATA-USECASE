import json
import requests
from datetime import datetime, timedelta
import time
from google.cloud import secretmanager




def fetch_secret(secret_id):
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/952702250749/secrets/freecurrencyAPI-key/versions/latest"  
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")

API_KEY = fetch_secret("freecurrencyAPI-key")
base_url_currencies = 'https://api.freecurrencyapi.com/v1/currencies'
base_url_rates = 'https://api.freecurrencyapi.com/v1/historical'

def fetch_currency_data_for_range(API_KEY, base_url_currencies, base_url_rates, start_date, end_date):
   try:
       all_data = {}  # Create an empty dictionary to store fetched data
       current_date = start_date
       api_calls_count = 0  # Counter to keep track of API calls


       while current_date <= end_date:
           formatted_date = current_date.strftime('%Y-%m-%d')


           params = {
               'apikey': API_KEY, 
               'base_currency': 'INR',
               'date': formatted_date
           }
           response = requests.get(base_url_rates, params=params)
           rate_data = response.json()


           print(f'Rate data for {formatted_date}:')
           print(json.dumps(rate_data, indent=2)) 


           # Store fetched data in the dictionary
           all_data[formatted_date] = rate_data


           api_calls_count += 1
           if api_calls_count % 5 == 0:
               print("Taking a break for 60 seconds...")
               time.sleep(60)  # Delay after every 5 API calls


           current_date += timedelta(days=1)


       # Write all the fetched data to a JSON file
       with open('currency_data.json', 'w') as json_file:
           json.dump(all_data, json_file, indent=2)
       print("Data saved to currency_data.json")


   except Exception as e:
       print(f"Error fetching currency data: {str(e)}")


# Example date range (from January 1, 2019, to January 10, 2019)
start_date = datetime(2019, 1, 1)
end_date = datetime(2019, 1, 10)


fetch_currency_data_for_range(API_KEY, base_url_currencies, base_url_rates, start_date, end_date)
