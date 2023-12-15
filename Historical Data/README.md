# COA-DATA-USECASE-
1. HISTORICAL ONE TIME DATA:

Currency Data Fetcher
This script retrieves historical currency exchange rate data using the Free Currency API and saves it into a JSON file.

Setup:

Free Currency API Key

Replace the placeholder 'FreecurrencyAPI" in the script with your valid Free Currency API key.

Python Libraries:

Ensure you have the required Python libraries installed:
json
requests
datetime
time

#Usage
The script fetch_currency_data_for_range() fetches historical currency exchange rates within a specified date range and stores the data in a JSON file named currency_data.json.

Example usage:

# Set the date range
start_date = datetime(2019, 1, 1)
end_date = datetime(2019, 1, 10)

# Fetch currency data for the specified date range
fetch_currency_data_for_range(API_KEY, base_url_currencies, base_url_rates, start_date, end_date)

<!-- Important Notes -->
API Key: Make sure to replace API_KEY variable with your Free Currency API key.
Rate Limit: The Free Currency API may have rate limits. The script includes logic to wait for 60 seconds after every 5 API calls to avoid hitting rate limits.
