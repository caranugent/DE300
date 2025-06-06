from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests, pandas as pd, time, boto3, os, pendulum
import matplotlib.pyplot as plt


S3_BUCKET = "nugent-bucket"
S3_PREFIX = "weather_data/"
WEATHER_STATIONS = ["KORD", "KENW", "KMDW", "KPNT"]
OUTPUT_PREFIX = "output_graphs/"

# Default arguments dictionary for the DAG execution
default_args = {
    'owner': 'airflow',  # Owner of the DAG
    'depends_on_past': False,  # Ensures tasks do not depend on past runs
    'start_date': pendulum.today('UTC').add(days=-1),  # DAG start date (yesterday)
    'retries': 1,  # Number of retry attempts upon failure
}


# function to generate a daily dashboard
def generate_dashboard():
    s3 = boto3.client("s3")

    # get data from S3 bucket that was uploaded in task 1
    objs = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_PREFIX).get("Contents", [])
    dfs = []

    ## CHECK IF 48 ROWS OF CSV FILES AVAILABLE --> IF NOT RETURN
    csv_keys = [obj["Key"] for obj in objs if obj["Key"].endswith(".csv")]

    dfs = []

    # If enough data, proceed to download and process

    # if the data is a .csv file, extract it
    for key in csv_keys:
        body = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"]
        df = pd.read_csv(body)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        dfs.append(df)

    if not dfs:
        return

    # organize data by date
    full_df = pd.concat(dfs)
    full_df["date"] = full_df["timestamp"].dt.date

    # TODAY SHOULD BE THE LAST TIME IN THE FILE: sort by timestamp
    full_df = full_df.sort_values(by="timestamp")

    # today_df IS LAST 24 HOURS OF DATA (grab last 12 files)
    today =  full_df.tail(1)["timestamp"].dt.date

    today_df = full_df.tail(12)

    # plot the three desired plots for today's data
    for col in ["temperature", "visibility", "relativeHumidity"]:
        plt.figure()
        for station in today_df["station"].unique():
            subset = today_df[today_df["station"] == station]
            plt.plot(subset["timestamp"], subset[col], label=station)
        plt.title(f"{col} on {today}")
        plt.xlabel("Time")
        plt.ylabel(col)
        plt.legend()
        output_path = f"/tmp/{col}_{today}.png"
        plt.savefig(output_path)

        s3.upload_file(output_path, S3_BUCKET, f"{OUTPUT_PREFIX}{col}_{today}.png")
        os.remove(output_path)

# set up the DAG:
daily_weather_dashboard_dag = DAG(
    'daily_weather_dashboard',                         # Name of the DAG
    default_args=default_args,
    description='Generate daily weather dashboard',
    schedule="@daily",                                 # Schedule interval for DAG execution to be daily
    # start_date=datetime(2025, 6, 5),                 # Run the DAG for 2 days
    # end_date=datetime(2025, 6, 7),
    catchup=False,
    tags=["dashboard"]  # DAG tagging for categorization
)

# CALL THIS LATER
task2 = PythonOperator(
    task_id="generate_daily_dashboard",
    python_callable=generate_dashboard,
    dag = daily_weather_dashboard_dag
)