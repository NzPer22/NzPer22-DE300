from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum

import boto3
import pandas as pd
import numpy as np
import os
import io
from datetime import datetime
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import OneHotEncoder

default_args = {
    "owner": "Team Adams",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2025, 6, 6, tz="UTC"),
    "retries": 1
}

S3_BUCKET = "adams-agrawal-evensen-mwaa"
WEATHER_DIR = "weather_data"
PREDICTIONS_DIR = "predictions"

def train_and_predict():
    s3_client = boto3.client("s3")
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=WEATHER_DIR)
    files = [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".csv")]

    all_dfs = []
    for file in files:
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=file)
        df = pd.read_csv(io.BytesIO(obj["Body"].read()))
        all_dfs.append(df)

    if not all_dfs:
        return

    df = pd.concat(all_dfs, ignore_index=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df.dropna(subset=["temperature"], inplace=True)
    df["hour"] = df["timestamp"].dt.hour
    df["minute"] = df["timestamp"].dt.minute

    encoder = OneHotEncoder(sparse_output=False).fit(df[["station"]])
    station_encoded = encoder.transform(df[["station"]])

    features = np.hstack([
        station_encoded,
        df[["dewpoint", "windSpeed", "barometricPressure", "visibility", "relativeHumidity"]].fillna(0),
        df[["hour", "minute"]]
    ])

    target = df["temperature"]

    model = LinearRegression()
    model.fit(features, target)

    now = datetime.utcnow()
    future_times = pd.date_range(now, periods=16, freq="30min")

    prediction_rows = []
    for station in ["KORD", "KENW", "KMDW", "KPNT"]:
        for time in future_times:
            hour = time.hour
            minute = time.minute
            avg_features = df[["dewpoint", "windSpeed", "barometricPressure", "visibility", "relativeHumidity"]].mean().values
            station_onehot = encoder.transform([[station]])[0]
            full_features = np.hstack([station_onehot, avg_features, [hour, minute]])
            predicted_temp = model.predict([full_features])[0]

            prediction_rows.append({
                "prediction_time": time,
                "station": station,
                "predicted_temp": predicted_temp
            })

    pred_df = pd.DataFrame(prediction_rows)
    filename = f"predictions_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}.csv"
    local_path = f"/tmp/{filename}"
    pred_df.to_csv(local_path, index=False)

    s3_key = os.path.join(PREDICTIONS_DIR, filename)
    s3_client.upload_file(local_path, S3_BUCKET, s3_key)

with DAG(
    dag_id="regression_prediction_dag",
    default_args=default_args,
    description="Train regression model and generate temperature predictions",
    schedule_interval="0 */20 * * *",
    catchup=False,
    tags=["weather", "ml"]
) as dag:

    task_train_predict = PythonOperator(
        task_id="task_train_predict",
        python_callable=train_and_predict
    )
