# ✈ Flight Analytics Platform

Real-time flight tracking and analytics platform powered by **OpenSky Network**, **Apache Kafka**, **PySpark Structured Streaming**, **PostgreSQL**, and a **Flask + Plotly** interactive dashboard.

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────┐
│                        DATA FLOW                                      │
├──────────────────────────────────────────────────────────────────────┤
│                                                                        │
│   OpenSky Network API  (poll every 10s)                               │
│          │                                                             │
│          ▼                                                             │
│   ┌─────────────────┐                                                 │
│   │  Kafka Producer  │  Python · confluent-kafka                      │
│   │  (producer/)     │  → publishes to topic: flight_stream           │
│   └────────┬────────┘                                                 │
│            │                                                           │
│            ▼                                                           │
│   ┌─────────────────┐                                                 │
│   │   Apache Kafka   │  topic: flight_stream  (3 partitions)          │
│   └────────┬────────┘                                                 │
│            │                                                           │
│            ▼                                                           │
│   ┌─────────────────┐                                                 │
│   │  PySpark ETL     │  Structured Streaming · micro-batch 15s        │
│   │  (spark/)        │  • parse JSON                                  │
│   │                  │  • derive features                             │
│   │                  │  • write to 3 PG schemas                       │
│   └────────┬────────┘                                                 │
│            │                                                           │
│            ▼                                                           │
│   ┌─────────────────────────────────────────────────────┐            │
│   │  PostgreSQL Data Warehouse                           │            │
│   │  ├── raw.flight_events_raw      (JSON landing)      │            │
│   │  ├── staging.flight_events_clean (normalised)        │            │
│   │  └── main.flight_analytics      (aggregated)         │            │
│   └───────────────────┬─────────────────────────────────┘            │
│                        │                                               │
│            ┌──────────┴────────────────────────────┐                 │
│            │                                         │                │
│            ▼                                         ▼                │
│   ┌─────────────────┐                    ┌──────────────────┐        │
│   │  Flask Dashboard │                    │  Jupyter Notebooks│        │
│   │  (dashboard/)    │                    │  (notebooks/)     │        │
│   │  • 9 Plotly charts                   │  • EDA            │        │
│   │  • ML results                        │  • Feature Eng.   │        │
│   │  • Auto-refresh                      │  • ML Training    │        │
│   │  port: 5000                          │  • Visual Analytics│        │
│   └─────────────────┘                    │  port: 8888       │        │
│                                           └──────────────────┘        │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Services

| Service        | Image / Build  | Port  | Description                           |
|----------------|---------------|-------|---------------------------------------|
| `zookeeper`    | confluentinc  | —     | Zookeeper for Kafka coordination      |
| `kafka`        | confluentinc  | 9092  | Kafka broker                          |
| `postgres`     | postgres:15   | 5432  | PostgreSQL data warehouse             |
| `producer`     | ./producer    | —     | OpenSky → Kafka publisher             |
| `spark`        | ./spark       | —     | PySpark Structured Streaming ETL      |
| `dashboard`    | ./dashboard   | 5000  | Flask + Plotly interactive dashboard  |
| `jupyter`      | jupyter/scipy | 8888  | Jupyter Lab for notebooks             |

---

## Project Structure

```
flight-analytics-platform/
├── docker-compose.yml          # Orchestration
├── .env.example                # Environment template  ← copy to .env
│
├── producer/                   # Kafka producer service
│   ├── producer.py
│   ├── requirements.txt
│   └── Dockerfile
│
├── spark/                      # PySpark streaming ETL
│   ├── streaming_job.py
│   ├── requirements.txt
│   └── Dockerfile
│
├── dashboard/                  # Flask web dashboard
│   ├── app.py
│   ├── charts.py               # Plotly figure builders
│   ├── db.py                   # PostgreSQL query layer
│   ├── requirements.txt
│   ├── Dockerfile
│   └── templates/
│       └── dashboard.html      # Responsive single-page dashboard
│
├── ml/                         # Machine learning pipeline
│   ├── train_model.py
│   ├── model_utils.py
│   ├── requirements.txt
│   └── models/                 # Saved .pkl files (git-ignored)
│
├── notebooks/                  # Jupyter notebooks
│   ├── 01_data_exploration.ipynb
│   ├── 02_feature_engineering.ipynb
│   ├── 03_machine_learning.ipynb
│   └── 04_visual_analytics.ipynb
│
├── database/
│   ├── init.sql                # Schemas + extensions
│   └── schema.sql              # Tables + indexes
│
└── shared/
    ├── config.py               # Centralised config (dataclasses)
    └── schema_mapping.py       # OpenSky field mapping + classifiers
```

---

## Quick Start

### 1. Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) ≥ 24
- [Docker Compose](https://docs.docker.com/compose/) ≥ 2.20 (bundled with Docker Desktop)
- At least **4 GB RAM** allocated to Docker (Spark needs ~2 GB)
- Internet access (to call the OpenSky API)

### 2. Clone / enter the project

```bash
cd flight-analytics-platform
```

### 3. Configure `environment`

```bash
# Copy the example file and edit it if needed
cp .env.example .env
```

The defaults in `.env.example` work out-of-the-box for a local setup.
The only value you might want to change is `FLASK_SECRET_KEY`.

### 4. Start everything

```bash
docker compose up --build
```

First build takes **5–10 minutes** (PySpark + Java image is large).
Subsequent starts are much faster.

### 5. Access services

| Application           | URL                            |
|-----------------------|--------------------------------|
| ✈ Flight Dashboard    | http://localhost:5000          |
| 📓 Jupyter Lab        | http://localhost:8888/lab      |
| 🐘 PostgreSQL (psql)  | `localhost:5432`               |
| 📨 Kafka              | `localhost:9092`               |

> **Tip:** The dashboard shows "Waiting for data…" for the first ~60 seconds
> while the producer collects initial batches and Spark processes them.

---

## Configuration Reference (`.env`)

```dotenv
# PostgreSQL
POSTGRES_DB=flightdw
POSTGRES_USER=flightuser
POSTGRES_PASSWORD=flightpass123
POSTGRES_HOST=postgres          # Docker service name — do not change
POSTGRES_PORT=5432

# Kafka
KAFKA_BROKER=kafka:29092        # Internal broker address — do not change

# Spark
SPARK_MASTER=local[*]

# OpenSky API
OPENSKY_API_URL=https://opensky-network.org/api/states/all

# Flask
FLASK_SECRET_KEY=change-me-in-production

# ML models path (inside dashboard container)
MODEL_PATH=/app/models
```

---

## Dashboard Features

The Flask dashboard at **http://localhost:5000** provides:

| Chart                           | Description                                       |
|---------------------------------|---------------------------------------------------|
| 🌍 Global Flight Map            | Interactive scatter-geo coloured by speed         |
| ✈ Flights by Country            | Horizontal bar chart, top 25 countries            |
| 🚀 Velocity Distribution        | Histogram of speed buckets (km/h)                 |
| 🛫 Altitude Distribution        | Donut chart by altitude band                      |
| 📈 Flights Over Time            | 24 h / 48 h time series with speed overlay       |
| 🌡️ Aircraft Density Heatmap    | Mapbox density heatmap                            |
| 🤖 Flight Pattern Clusters      | ML cluster scatter (speed vs altitude)            |
| ⚠️ Anomaly Alerts               | Bar chart of highest-anomaly-score flights        |
| 🗺️ Region Radar                 | Radar chart of traffic by world region            |

**Controls:**
- Filter by origin country via the dropdown
- Change time range (6 h → 48 h)
- Dashboard auto-refreshes every **30 seconds**
- Click **Run ML Prediction** to test the velocity-prediction endpoint

---

## Machine Learning

Three models are trained in `ml/train_model.py`:

| Model                         | Algorithm                  | Target                     |
|-------------------------------|----------------------------|----------------------------|
| Velocity Predictor            | GradientBoostingRegressor  | `avg_velocity_kmh`         |
| Flight Pattern Clusterer      | KMeans (k=6)               | Pattern group              |
| Anomaly Detector              | IsolationForest            | Anomaly score [0–1]        |

### Train models manually

```bash
# Run inside the running spark/dashboard container or on host:
cd ml
pip install -r requirements.txt

# Set DB variables
export POSTGRES_HOST=localhost
export POSTGRES_DB=flightdw
export POSTGRES_USER=flightuser
export POSTGRES_PASSWORD=flightpass123

python train_model.py           # train all 3 models
python train_model.py --model v # velocity only
python train_model.py --model c # clustering only
python train_model.py --model a # anomaly only
```

Models are saved to `ml/models/` and automatically picked up by the dashboard
(via the `./ml/models:/app/models` volume mount).

---

## Jupyter Notebooks

Open **http://localhost:8888/lab** and navigate to the `work/` folder.

| Notebook                       | Content                                             |
|--------------------------------|-----------------------------------------------------|
| `01_data_exploration.ipynb`    | Row counts, descriptive stats, missing values, maps |
| `02_feature_engineering.ipynb` | Velocity bins, altitude bands, heading analysis     |
| `03_machine_learning.ipynb`    | Full ML pipeline with evaluation and visualisation  |
| `04_visual_analytics.ipynb`    | Density maps, globe, radar, bar race                |

---

## Data Warehouse Schemas

```
raw              staging                  main
─────────────    ────────────────────    ─────────────────────────
flight_events_raw  flight_events_clean    flight_analytics
                                          country_traffic_summary
                                          hourly_flight_counts
                                          ml_model_registry
```

Connect with any Postgres client:

```
Host:     localhost
Port:     5432
Database: flightdw
User:     flightuser
Password: flightpass123
```

### Sample queries

```sql
-- Active flights in the last 5 minutes
SELECT COUNT(DISTINCT icao24), AVG(avg_velocity_kmh)
FROM main.flight_analytics
WHERE last_updated >= NOW() - INTERVAL '5 minutes';

-- Top countries
SELECT origin_country, SUM(flight_count) AS flights
FROM main.country_traffic_summary
WHERE snapshot_time >= NOW() - INTERVAL '1 hour'
GROUP BY 1 ORDER BY 2 DESC LIMIT 10;

-- Anomalies
SELECT icao24, callsign, anomaly_score
FROM main.flight_analytics
WHERE anomaly_score >= 0.7
ORDER BY anomaly_score DESC;
```

---

## Stopping and Cleaning Up

```bash
# Stop all services (preserves data)
docker compose down

# Stop and delete all data volumes
docker compose down -v

# Rebuild from scratch
docker compose down -v && docker compose up --build
```

---

## Known Limitations & Notes

- **OpenSky rate limits**: The public (unauthenticated) API allows ~1 request/10 s.
  If you receive 429 errors, the producer backs off automatically.
- **Spark startup time**: PySpark takes ~30–45 s to initialise. During this window
  the producer publishes to Kafka but ETL hasn't started yet — data is buffered.
- **First-time data**: The dashboard requires at least one Spark micro-batch (~15 s)
  before charts populate. Empty charts display a "Waiting for data…" placeholder.
- **ML models on fresh install**: ML models are only available after running
  `train_model.py`. The dashboard gracefully shows empty cluster/anomaly charts
  until models are loaded.

---

## License

MIT — free for educational and commercial use.
