# Weather Forecasting with Apache Airflow

This project implements a weather data pipeline using Apache Airflow and AWS S3 to automate collection, analysis, and visualization of weather data from four NOAA stations over a 48-hour window.

## 🗂 Overview of DAGs

### 1. `weather_collection_dag.py`
**Purpose:**  
Collects the latest weather observations every 2 hours from the [NOAA API](https://www.weather.gov/documentation/services-web-api) for the following stations:  
- KORD (Chicago O'Hare)
- KENW (Kenosha Regional)
- KMDW (Chicago Midway)
- KPNT (Pontiac)

**Key Features:**
- Extracts fields like temperature, humidity, visibility, wind speed, etc.
- Saves the data to timestamped `.csv` files.
- Uploads these files to your S3 bucket under `weather_data/`.

**Schedule:** Every 2 hours (`0 */2 * * *`)

---

### 2. `model_training_dag.py`
**Purpose:**  
Trains a linear regression model to forecast temperature values using historical data.

**Key Features:**
- Runs after 20 and 40 hours of data have been collected.
- Uses station and hour-index features to train the model.
- Predicts future temperature over an 8-hour window in 30-minute intervals.
- Saves prediction output to S3 under `predictions/`.

**Schedule:** Every 20 hours (`timedelta(hours=20)`)

---

### 3. `weather_dashboard_dag.py`
**Purpose:**  
Generates visual dashboards for key weather metrics:  
- Temperature (°C)  
- Visibility (m)  
- Relative Humidity (%)

**Key Features:**
- Aggregates data for each day.
- Plots time series for each station.
- Uploads `.png` files to S3 under `output_graphs/`.

**Schedule:** Daily (`@daily`)

---

## 🔧 Setup Notes

- All DAGs assume an S3 bucket: `adams-agrawal-evensen-mwaa`
- AWS region: `us-east-1`
- AWS credentials must be configured for access via Airflow (e.g., via execution role).
- Required Python packages are listed in `requirements.txt`:

