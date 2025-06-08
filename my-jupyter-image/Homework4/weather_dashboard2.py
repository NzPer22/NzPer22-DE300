from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum

import boto3
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os
from pathlib import Path

default_args = {
    "owner": "AdamsTeam",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2025, 6, 6, tz="UTC"),
    "retries": 1,
}

SCHEDULE = "0 3 * * *"
BUCKET = "adams-agrawal-evensen-mwaa"
INPUT_PREFIX = "weather_data"
OUTPUT_PREFIX = "output_graphs"

def generate_daily_weather_visuals():
    s3 = boto3.client("s3")
    s3_resource = boto3.resource("s3")
    bucket = s3_resource.Bucket(BUCKET)

    target_date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

    tmp_dir = Path("/tmp/daily_weather_files")
    tmp_dir.mkdir(parents=True, exist_ok=True)

    local_csvs = []
    for obj in bucket.objects.filter(Prefix=INPUT_PREFIX):
        if obj.key.endswith(".csv"):
            local_file = tmp_dir / Path(obj.key).name
            bucket.download_file(obj.key, str(local_file))
            local_csvs.append(local_file)

    all_dfs = []
    for file in local_csvs:
        df = pd.read_csv(file)
        if df.empty:
            continue
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df["date"] = df["timestamp"].dt.date.astype(str)
        if target_date in df["date"].values:
            all_dfs.append(df[df["date"] == target_date])

    if not all_dfs:
        return

    df_all = pd.concat(all_dfs).sort_values("timestamp")

    label_map = {
        "temperature": "Temperature (°C)",
        "visibility": "Visibility (m)",
        "relativeHumidity": "Humidity (%)"
    }

    for metric in label_map:
        plt.figure(figsize=(10, 5))
        for station, subset in df_all.groupby("station"):
            plt.plot(
                subset["timestamp"],
                subset[metric],
                label=station,
                marker='o',
                markersize=6,
                linewidth=2
            )

        plt.title(f"{metric} - {target_date}", fontsize=16)
        plt.xlabel("Time (UTC)", fontsize=12)
        plt.ylabel(label_map[metric], fontsize=12)
        plt.xticks(fontsize=10)
        plt.yticks(fontsize=10)
        plt.legend()
        plt.tight_layout()

        ax = plt.gca()
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        plt.setp(ax.get_xticklabels(), rotation=30)

        file_name = f"{metric}_{target_date}.png"
        local_plot = tmp_dir / file_name
        plt.savefig(local_plot)
        plt.close()

        s3_key = f"{OUTPUT_PREFIX}/{file_name}"
        print(f"Uploading {local_plot} to s3://{BUCKET}/{s3_key}")
        s3.upload_file(str(local_plot), BUCKET, s3_key)

with DAG(
    dag_id="daily_weather_dashboard",
    default_args=default_args,
    description="Plot daily weather dashboard and upload to S3",
    schedule=SCHEDULE,
    catchup=False,
    tags=["weather", "dashboard"],
    end_date=datetime(2025, 6, 7),
) as dag:

    plot_weather = PythonOperator(
        task_id="generate_and_upload_dashboard",
        python_callable=generate_daily_weather_visuals
    )
