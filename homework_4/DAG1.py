from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests, pandas as pd, time, boto3, os, pendulum

S3_BUCKET = "nugent-bucket"
S3_PREFIX = "weather_data/"
BASE_URL = "https://api.weather.gov/stations/{station}/observations/latest"
WEATHER_STATIONS = ["KORD", "KENW", "KMDW", "KPNT"]

start = pendulum.now('UTC').replace(minute=0, second=0, microsecond=0)
end = start + timedelta(hours=48)

# Default arguments dictionary for the DAG execution
default_args = {
    'owner': 'airflow',  # Owner of the DAG
    'depends_on_past': False,  # Ensures tasks do not depend on past runs
    'start_date': pendulum.today('UTC').add(days=-1),  # DAG start date (yesterday)
    'retries': 1,  # Number of retry attempts upon failure
}

# function to collect weather data: 
def collect_weather():
    now = datetime.utcnow().isoformat()

    collected_data = {}
    for station in WEATHER_STATIONS:
        url = BASE_URL.format(station=station)
        response = requests.get(url)
        if response.status_code == 200:
            
            json_data = response.json()

            # get properties from the json request
            props = json_data.get("properties", {})

            # extract all desired fields into row 
            row = {
                "timeOfCollection": now,
                "timestamp": props.get("timestamp"),
                "station": station,
                "temperature": props.get("temperature", {}).get("value"),
                "dewpoint": props.get("dewpoint", {}).get("value"),
                "windSpeed": props.get("windSpeed", {}).get("value"),
                "barometricPressure": props.get("barometricPressure", {}).get("value"),
                "visibility": props.get("visibility", {}).get("value"),
                "precipitationLastHour": props.get("precipitationLastHour", {}).get("value"),
                "relativeHumidity": props.get("relativeHumidity", {}).get("value"),
                "heatIndex": props.get("heatIndex", {}).get("value"),
            }

            # add row as entry for given station
            collected_data[station] = row
        else:
            collected_data[station] = {"error": "Failed to fetch data"}
        time.sleep(2)

    
    # save data as a dataframe
    df = pd.DataFrame.from_dict(collected_data, orient="index")


    # make filename specific to time so it does not overwrite
    filename = f"weather_data_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}.csv"
    local_path = f"/tmp/{filename}"

    # save dataframe as a CSV
    df.to_csv(local_path, index=False)

    # upload to S3 bucket
    s3 = boto3.client("s3")
    s3.upload_file(local_path, S3_BUCKET, f"{S3_PREFIX}{filename}")

    # get rid of temp path
    os.remove(local_path)



# set up the DAG:
weather_collection_dag = DAG(
    'collect_weather_data',                             # Name of the DAG
    default_args=default_args,
    description='Collect weather data every 2 hours',
    schedule="0 */2 * * *",                             # Schedule interval for DAG execution as every 2 hours
    start_date=start,                    # Schedule to run for 2 days
    end_date=end,
    catchup=False,
    tags=["weather"]  # DAG tagging for categorization
)

# CALL THIS LATER
task1 = PythonOperator(
    task_id="collect_and_store_data",
    python_callable=collect_weather,
    dag = weather_collection_dag
)
