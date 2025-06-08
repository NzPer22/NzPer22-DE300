from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import boto3
import time
from pathlib import Path
import pendulum

LOCAL_TZ = pendulum.timezone("America/Chicago")
START_DATE = datetime(2025, 6, 7, 0, 0, tzinfo=LOCAL_TZ)
WORKFLOW_SCHEDULE = "0 */2 * * *"

default_args = {
    "owner": "Team Adams",
    "start_date": START_DATE,
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

def fetch_and_store_weather():
    stations = ["KORD", "KENW", "KMDW", "KPNT"]
    base_url = "https://api.weather.gov/stations/{}/observations/latest"
    bucket = "adams-agrawal-evensen-mwaa"
    s3_prefix = "weather_data"

    records = []
    timestamp_collected = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    s3 = boto3.client("s3")

    for station in stations:
        try:
            response = requests.get(base_url.format(station), timeout=10)
            if response.status_code == 200:
                props = response.json().get("properties", {})
                records.append({
                    "timeOfCollection": timestamp_collected,
                    "timestamp": props.get("timestamp"),
                    "station": station,
                    "temperature": props.get("temperature", {}).get("value"),
                    "dewpoint": props.get("dewpoint", {}).get("value"),
                    "windSpeed": props.get("windSpeed", {}).get("value"),
                    "barometricPressure": props.get("barometricPressure", {}).get("value"),
                    "visibility": props.get("visibility", {}).get("value"),
                    "precipitationLastHour": props.get("precipitationLastHour", {}).get("value"),
                    "relativeHumidity": props.get("relativeHumidity", {}).get("value"),
                    "heatIndex": props.get("heatIndex", {}).get("value")
                })
            else:
                print(f"Station {station} request failed (HTTP {response.status_code})")
        except Exception as e:
            print(f"Error fetching data from {station}: {e}")
        time.sleep(1.5)

    if not records:
        raise ValueError("No records collected from any station.")

    df = pd.DataFrame(records)
    fname = f"obs_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}.csv"
    local_path = Path(f"/tmp/{fname}")
    df.to_csv(local_path, index=False)

    s3_key = f"{s3_prefix}/{fname}"
    s3.upload_file(str(local_path), bucket, s3_key)
    print(f"Uploaded to s3://{bucket}/{s3_key}")

with DAG(
    dag_id="upload_weather_observations",
    default_args=default_args,
    description="Collect weather data and upload to S3 every 2 hours",
    schedule=WORKFLOW_SCHEDULE,
    catchup=False,
    tags=["weather", "s3", "api"]
) as dag:

    run_upload = PythonOperator(
        task_id="upload_weather_to_s3",
        python_callable=fetch_and_store_weather
    )
