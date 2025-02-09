import logging
import azure.functions as func
from azure.eventhub import EventHubProducerClient, EventData
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import requests
import json

app = func.FunctionApp()

@app.timer_trigger(schedule="0 0 */2 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def weatherapifunction(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.') 

    
    #event hub config
    
    EVENT_HUB_NAME = "weatherstreamg"

    EVENT_HUB_NAMESPACE = "namespace.servicebus.windows.net" 

    # Uses Managed Identity of Function App
    credential = DefaultAzureCredential()
    

    #intialize event producer
    producer = EventHubProducerClient(
        fully_qualified_namespace=EVENT_HUB_NAMESPACE,
        eventhub_name=EVENT_HUB_NAME,
        credential=credential
    )

    def send_event(event):
        event_data_batch=producer.create_batch()
        event_data_batch.add(EventData(json.dumps(event)))
        producer.send_batch(event_data_batch)

    # handle the API response
    def handle_response(response):
        if response.status_code == 200:
            return response.json()
        else:
            return f"Error: {response.status_code}, {response.text}"

    # get current weather and air quality data
    def get_curr_weather(baseurl, api_key, location):
        curr_weather_url = f"{baseurl}/current.json"
        params = {
            'key': api_key,
            'q': location,
            "aqi": 'yes'
        }
        response = requests.get(curr_weather_url, params=params)
        return handle_response(response)

    # Function to get Forecast Data
    def get_forecast_weather(baseurl, api_key, location, days):
        forecast_url = f"{baseurl}/forecast.json"
        params = {
            "key": api_key,
            "q": location,
            "days": days,
        }
        response = requests.get(forecast_url, params=params)
        return handle_response(response)

    # Function to get Alerts
    def get_alerts(baseurl, api_key, location):
        alerts_url = f"{baseurl}/alerts.json"
        params = {
            'key': api_key,
            'q': location,
            "alerts": 'yes'
        }
        response = requests.get(alerts_url, params=params)
        return handle_response(response)

    # Flatten and merge the data
    def flatten_data(curr_weather, forecast_weather, alerts):
        location_data = curr_weather.get("location", {})
        current = curr_weather.get("current", {})
        condition = current.get("condition", {})
        air_quality = current.get("air_quality", {})
        forecast = forecast_weather.get("forecast", {}).get("forecastday", [])
        alert_list = alerts.get("alerts", {}).get("alert", [])

        flattened_data = {
            'name': location_data.get('name'),
            'region': location_data.get('region'),
            'country': location_data.get('country'),
            'lat': location_data.get('lat'),
            'lon': location_data.get('lon'),
            'localtime': location_data.get('localtime'),
            'temp_c': current.get('temp_c'),
            'is_day': current.get('is_day'),
            'condition_text': condition.get('text'),
            'condition_icon': condition.get('icon'),
            'wind_kph': current.get('wind_kph'),
            'wind_degree': current.get('wind_degree'),
            'wind_dir': current.get('wind_dir'),
            'pressure_in': current.get('pressure_in'),
            'precip_in': current.get('precip_in'),
            'humidity': current.get('humidity'),
            'cloud': current.get('cloud'),
            'feelslike_c': current.get('feelslike_c'),
            'uv': current.get('uv'),
            'air_quality': {
                'co': air_quality.get('co'),
                'no2': air_quality.get('no2'),
                'o3': air_quality.get('o3'),
                'so2': air_quality.get('so2'),
                'pm2_5': air_quality.get('pm2_5'),
                'pm10': air_quality.get('pm10'),
                'us-epa-index': air_quality.get('us-epa-index'),
                'gb-defra-index': air_quality.get('gb-defra-index')
            },
            'alerts': [
                {
                    'headline': alert.get('headline'),
                    'severity': alert.get('severity'),
                    'description': alert.get('desc'),
                    'instruction': alert.get('instruction')
                }
                for alert in alert_list
            ],
            'forecast': [
                {
                    'date': day.get('date'),
                    'maxtemp_c': day.get('day', {}).get('maxtemp_c'),
                    'mintemp_c': day.get('day', {}).get('mintemp_c'),
                    'condition': day.get('day', {}).get('condition', {}).get('text')
                }
                for day in forecast
            ]
        }
        return flattened_data
    
    #fetch vault credentials
    def get_secret_from_keyvault(vault_url, secret_name):
        credential = DefaultAzureCredential()
        secret_client = SecretClient(vault_url=vault_url, credential=credential)
        retrieved_secret = secret_client.get_secret(secret_name)
        return retrieved_secret.value

    #function to weather data
    def fetch_weather_data():

        baseurl = "http://api.weatherapi.com/v1/"
        location = "Mumbai"  
        
        vault_url = "https://vaultname.vault.azure.net/"
        api_secret_name = "weatherapikey"
        weatherapikey = get_secret_from_keyvault(vault_url, api_secret_name)

        # Get data from API
        curr_weather = get_curr_weather(baseurl, weatherapikey, location)
        forecast_weather = get_forecast_weather(baseurl, weatherapikey, location, 3)
        alerts = get_alerts(baseurl, weatherapikey, location)

        # Flatten and merge data
        merged_data = flatten_data(curr_weather, forecast_weather, alerts)

        send_event(merged_data)

    fetch_weather_data()

