from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests, pandas as pd, time, boto3, os, pendulum
import matplotlib.pyplot as plt
from io import BytesIO
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import OneHotEncoder
from sklearn.impute import SimpleImputer


S3_BUCKET = "nugent-bucket"
S3_PREFIX = "weather_data/"
WEATHER_STATIONS = ["KORD", "KENW", "KMDW", "KPNT"]
OUTPUT_PREFIX = "predictions/"

# Default arguments dictionary for the DAG execution
default_args = {
    'owner': 'airflow',  # Owner of the DAG
    'depends_on_past': False,  # Ensures tasks do not depend on past runs
    'start_date': pendulum.today('UTC').add(days=-1),  # DAG start date (yesterday)
    'retries': 1,  # Number of retry attempts upon failure
}

# function to train and predict temperature
def train_10_20():

    # get data from S3 bucket that was uploaded in task 1
    s3 = boto3.client("s3")
    objs = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_PREFIX).get("Contents", [])

    ## CHECK IF 10 OR 20 ROWS OF CSV FILES AVAILABLE --> IF NOT RETURN
    csv_keys = [obj["Key"] for obj in objs if obj["Key"].endswith(".csv")]

    # NOTE: COMMENTED OUT DATA FOR SCHEDULED IMPLEMENTATION
        # size = len(csv_keys)
        # if size >= 10 and size < 20 :
        #     train_predict(s3, csv_keys[0:9])
        # else if size > 20:
        #     train_predict(s3, csv_keys[0:19])
        # else:
        #     return
    
    # RUN W/ 10:
    train_predict(s3, csv_keys[0:9])

    # RUN W/ 20:
    train_predict(s3, csv_keys[0:19])



def train_predict(s3, csv_keys):

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

    # combine data into 1 DF to use to train model -- drop rows missing temp or time
    full_df = pd.concat(dfs).dropna(subset=["temperature", "timestamp"])

    # SORT BY TIMESTAMP
    full_df = full_df.sort_values(by="timestamp")

    # add hour feature to be used
    full_df["hour"] = full_df["timestamp"].dt.hour + full_df["timestamp"].dt.minute / 60.0
    
    X = full_df[[ "station", "dewpoint", "windSpeed", "barometricPressure", "visibility", "relativeHumidity", "heatIndex", "hour" ]]
    y = full_df["temperature"]

    # mean imputation:
    numeric_features = X.drop("station", axis=1)
    imputer = SimpleImputer(strategy="mean")
    X_numeric_imputed = imputer.fit_transform(numeric_features)

    # set up one-hot encoding for station (categorical data)
    enc = OneHotEncoder(sparse_output=False)
    X_station_encoded = enc.fit_transform(X[["station"]])
    X = np.hstack([X_station_encoded, X_numeric_imputed])

    # linear regression model to predict temperature
    model = LinearRegression().fit(X, y)

    preds = []

    # get timestamp from last value, but sort by timestamp first
    now = full_df["timestamp"].max()

    # for each station, predict the temperature for the next 8 hours (in 30-minute increments)
    for station in full_df["station"].unique():
        # 8 hours in 30 min intervals = 16 intervals
        station_data = full_df[full_df["station"] == station]
        for i in range(1, 17):  
            future_hour = now + timedelta(minutes=30 * i)
            # future_hour = now + i * 0.5

            # use mean of past values
            row = {
                "station": station,
                "dewpoint": station_data["dewpoint"].mean(),
                "windSpeed": station_data["windSpeed"].mean(),
                "barometricPressure": station_data["barometricPressure"].mean(),
                "visibility": station_data["visibility"].mean(),
                "relativeHumidity": station_data["relativeHumidity"].mean(),
                "heatIndex": station_data["heatIndex"].mean(),
                "hour": future_hour.hour + future_hour.minute / 60.0
            }

            # replace NaNs
            row = {k: (0 if pd.isna(v) else v) for k, v in row.items()}

            # Encode and transform input
            station_df = pd.DataFrame([[row["station"]]], columns=["station"])
            station_encoded = enc.transform(station_df)

            numeric_vals = np.array([ row["dewpoint"], row["windSpeed"], row["barometricPressure"], row["visibility"], row["relativeHumidity"],row["heatIndex"],row["hour"]]).reshape(1, -1)

            numeric_imputed = imputer.transform(numeric_vals)

            # set up row for prediction
            x_row = np.hstack([station_encoded, numeric_imputed])

            # predict and store temperature
            pred_temp = model.predict(x_row)[0]
            row["predicted_temp"] = pred_temp
            row["prediction_time"] = datetime.utcnow().isoformat()
            preds.append(row)

    # convert to dataframe
    pred_df = pd.DataFrame(preds)

    # save predictions to a csv file with time
    filename = f"predictions_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}.csv"
    local_path = f"/tmp/{filename}"
    pred_df.to_csv(local_path, index=False)

    # upload the file to the S3 bucket
    s3.upload_file(local_path, S3_BUCKET, f"{OUTPUT_PREFIX}{filename}")
    os.remove(local_path)

# set up the DAG:
weather_model_dag = DAG(
    'train_weather_model',  # Name of the DAG
    default_args=default_args,
    description='Train linear regression model after 20h and 40h',
    # NOTE: SHOULD RUN EVERY 20 HOURS, BUT FOR THE PURPOSES OF THIS SCRIPT WE ARE RUNNING RETROSPECTIVELY ONCE WITH ALL OF THE DATA 
        # schedule="0 */20 * * *",  # Schedule interval for DAG execution as ever 20 hours
        # start_date=datetime(2025, 6, 5),
        # end_date=datetime(2025, 6, 7),
    schedule=None,
    catchup=False,
    tags=["model"]  # DAG tagging for categorization
)

# CALL THIS LATER
task3 = PythonOperator(
    task_id="train_and_predict",
    python_callable=train_10_20,
    dag = weather_model_dag
)