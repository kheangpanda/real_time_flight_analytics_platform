"""
shared/config.py
Centralised configuration loader for all services.
Values are read from environment variables; defaults are provided for local dev.
"""
import os
from dataclasses import dataclass, field
from typing import Optional


# ─────────────────────────────────────────────────────────────────────────────
# PostgreSQL
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class PostgresConfig:
    db: str = field(default_factory=lambda: os.getenv("POSTGRES_DB", "flightdw"))
    user: str = field(default_factory=lambda: os.getenv("POSTGRES_USER", "flightuser"))
    password: str = field(default_factory=lambda: os.getenv("POSTGRES_PASSWORD", "flightpass123"))
    host: str = field(default_factory=lambda: os.getenv("POSTGRES_HOST", "localhost"))
    port: int = field(default_factory=lambda: int(os.getenv("POSTGRES_PORT", "5432")))

    @property
    def url(self) -> str:
        return (
            f"postgresql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.db}"
        )

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.db}"

    @property
    def jdbc_props(self) -> dict:
        return {
            "user": self.user,
            "password": self.password,
            "driver": "org.postgresql.Driver",
        }


# ─────────────────────────────────────────────────────────────────────────────
# Kafka
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class KafkaConfig:
    broker: str = field(default_factory=lambda: os.getenv("KAFKA_BROKER", "localhost:9092"))
    topic: str = "flight_stream"
    group_id: str = "flight-analytics-group"
    auto_offset_reset: str = "earliest"

    @property
    def producer_config(self) -> dict:
        return {
            "bootstrap.servers": self.broker,
            "acks": "all",
            "retries": 5,
            "retry.backoff.ms": 1000,
            "compression.type": "snappy",
            "batch.size": 16384,
            "linger.ms": 100,
        }

    @property
    def consumer_config(self) -> dict:
        return {
            "bootstrap.servers": self.broker,
            "group.id": self.group_id,
            "auto.offset.reset": self.auto_offset_reset,
            "enable.auto.commit": "true",
        }


# ─────────────────────────────────────────────────────────────────────────────
# OpenSky
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class OpenSkyConfig:
    api_url: str = field(
        default_factory=lambda: os.getenv(
            "OPENSKY_API_URL",
            "https://opensky-network.org/api/states/all",
        )
    )
    poll_interval_sec: int = 10
    request_timeout_sec: int = 30
    max_retries: int = 5
    backoff_factor: float = 2.0


# ─────────────────────────────────────────────────────────────────────────────
# Flask
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class FlaskConfig:
    secret_key: str = field(
        default_factory=lambda: os.getenv(
            "FLASK_SECRET_KEY", "dev-secret-key-change-in-production"
        )
    )
    debug: bool = field(default_factory=lambda: os.getenv("FLASK_DEBUG", "0") == "1")
    host: str = "0.0.0.0"
    port: int = 5000
    refresh_interval_ms: int = 30_000
    max_table_rows: int = 500


# ─────────────────────────────────────────────────────────────────────────────
# ML
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class MLConfig:
    model_path: str = field(
        default_factory=lambda: os.getenv("MODEL_PATH", "./models")
    )
    velocity_model_file: str = "velocity_predictor.pkl"
    cluster_model_file: str = "flight_clusterer.pkl"
    anomaly_model_file: str = "anomaly_detector.pkl"
    scaler_file: str = "feature_scaler.pkl"
    min_training_rows: int = 100


# ─────────────────────────────────────────────────────────────────────────────
# Aggregated settings object
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class Settings:
    postgres: PostgresConfig = field(default_factory=PostgresConfig)
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    opensky: OpenSkyConfig = field(default_factory=OpenSkyConfig)
    flask: FlaskConfig = field(default_factory=FlaskConfig)
    ml: MLConfig = field(default_factory=MLConfig)


# Module-level singleton
settings = Settings()
