from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests, pandas as pd, time, boto3, os, pendulum
import matplotlib.pyplot as plt


S3_BUCKET = "nugent-bucket"
S3_PREFIX = "weather_data/"
WEATHER_STATIONS = ["KORD", "KENW", "KMDW", "KPNT"]
OUTPUT_PREFIX = "output_graphs/"

# NOTE: ideally the DAG would run at midnight each night with the data from that previous day
# but I was not able to implement this in code

# Default arguments dictionary for the DAG execution
default_args = {
    'owner': 'airflow',  # Owner of the DAG
    'depends_on_past': False,  # Ensures tasks do not depend on past runs
    'start_date': pendulum.today('UTC').add(days=-1),  # DAG start date (yesterday)
    'retries': 1,  # Number of retry attempts upon failure
}

# function to generate dashboard for a given date
def generate_date_dashboard(s3, date, df):
    # today_df all entries with same date
    today_df = df[df["timestamp"].dt.date == date]

    if today_df.empty:
        return

    # plot the three desired plots for today's data
    for col in ["temperature", "visibility", "relativeHumidity"]:
        plt.figure()
        for station in today_df["station"].unique():
            subset = today_df[today_df["station"] == station]
            plt.plot(subset["timestamp"], subset[col], label=station)
        plt.title(f"{col} on {date}")
        plt.xlabel("Time")
        plt.ylabel(col)
        plt.legend()
        output_path = f"/tmp/{col}_{date}.png"
        plt.savefig(output_path)

        s3.upload_file(output_path, S3_BUCKET, f"{OUTPUT_PREFIX}{col}_{date}.png")
        os.remove(output_path)


# function to generate each daily dashboard
def generate_all_dashboards():
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
    full_df["timestamp"] = pd.to_datetime(full_df["timestamp"])

    # TODAY SHOULD BE THE LAST TIME IN THE FILE: sort by timestamp
    full_df = full_df.sort_values(by="timestamp")

    # Get all unique dates in the dataset
    unique_dates = full_df["timestamp"].dt.date.unique()

    for date in unique_dates:
        generate_date_dashboard(full_df, date, s3)


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
    python_callable=generate_all_dashboards,
    dag = daily_weather_dashboard_dag
)