from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import json

'''
import os 
from dotenv import load_dotenv
import requests

# Load API key from .env file
load_dotenv("~/.env")  # Path to your .env file
API_KEY = os.getenv("OPENWEATHER_API_KEY")
'''

from airflow.models import Variable

API_KEY = Variable.get("OPENWEATHER_API_KEY")

print(f"API_key: {API_KEY}")

city = "Portland"
url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}"

def load_aws_credentials(filepath='/home/ubuntu/airflow/dags/aws_credentials.json'):
    """Securely load AWS credentials from JSON file"""
    try:
        with open(filepath) as f:
            return json.load(f)
    except Exception as e:
        raise Exception(f"Failed to load credentials: {str(e)}")

def kelvin_to_fahrenheit(temp_in_kelvin):
    temp_in_fahrenheit = (temp_in_kelvin - 273.15) * (9/5) + 32
    return temp_in_fahrenheit

def transform_load_data(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_weather_data")
    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp"])
    feels_like_farenheit= kelvin_to_fahrenheit(data["main"]["feels_like"])
    min_temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp_min"])
    max_temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
    sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
    sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

    transformed_data = {"City": city,
                        "Description": weather_description,
                        "Temperature (F)": temp_farenheit,
                        "Feels Like (F)": feels_like_farenheit,
                        "Minimun Temp (F)":min_temp_farenheit,
                        "Maximum Temp (F)": max_temp_farenheit,
                        "Pressure": pressure,
                        "Humidty": humidity,
                        "Wind Speed": wind_speed,
                        "Time of Record": time_of_record,
                        "Sunrise (Local Time)":sunrise_time,
                        "Sunset (Local Time)": sunset_time                        
                        }
    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)
    aws_credentials = load_aws_credentials() #{"key": "xxxxxxxxx", "secret": "xxxxxxxxxx", "token": "xxxxxxxxxxxxxx"}

    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    dt_string = 'current_weather_data_portland_' + dt_string
    df_data.to_csv(f"s3://weatherapiairflow-12445/{dt_string}.csv", index=False, storage_options=aws_credentials)



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 8),
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2, # no. time of retries after failure
    'retry_delay': timedelta(minutes=2) # retry in 2 mins
}

with DAG('weather_dag', # unique name for all dag in this airflow 
    default_args=default_args,
    schedule = '@daily',
    catchup=False) as dag:

    is_weather_api_ready = HttpSensor(
        task_id ='is_weather_api_ready', # unique task id in the current dag
        http_conn_id='weathermap_api',
        endpoint=f'/data/2.5/weather?q={city}&appid={API_KEY}'
        )

    extract_weather_data = HttpOperator(
        task_id = 'extract_weather_data',
        http_conn_id = 'weathermap_api',
        endpoint=f'/data/2.5/weather?q={city}&appid={API_KEY}',
        method = 'GET',
        response_filter= lambda r: json.loads(r.text),
        log_response=True
        )

    transform_load_weather_data = PythonOperator(
        task_id= 'transform_load_weather_data',
        python_callable=transform_load_data
        )

    is_weather_api_ready >> extract_weather_data >> transform_load_weather_data