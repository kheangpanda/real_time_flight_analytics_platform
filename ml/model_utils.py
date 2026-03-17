"""
ml/model_utils.py
Shared utilities for feature engineering, model I/O, and evaluation.
"""
from __future__ import annotations

import logging
import os
import pathlib
from typing import Any, Dict, List, Optional, Tuple

import joblib
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler

logger = logging.getLogger(__name__)

MODEL_PATH = pathlib.Path(os.getenv("MODEL_PATH", "./models"))
MODEL_PATH.mkdir(parents=True, exist_ok=True)


# ─────────────────────────────────────────────────────────────────────────────
# Feature definitions
# ─────────────────────────────────────────────────────────────────────────────
VELOCITY_FEATURES = [
    "baro_altitude",
    "true_track",
    "vertical_rate",
    "on_ground_ratio",    # derived: on_ground_count / observation_count
]

CLUSTER_FEATURES = [
    "avg_velocity_kmh",
    "avg_altitude",
    "avg_vertical_rate",
    "observation_count",
]

ANOMALY_FEATURES = [
    "avg_velocity_kmh",
    "avg_altitude",
    "avg_vertical_rate",
    "max_velocity_kmh",
    "max_altitude",
]

TARGET_VELOCITY = "avg_velocity_kmh"


# ─────────────────────────────────────────────────────────────────────────────
# Feature engineering
# ─────────────────────────────────────────────────────────────────────────────
def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add derived columns to the raw dataframe."""
    df = df.copy()

    # on_ground ratio
    if "on_ground_count" in df.columns and "observation_count" in df.columns:
        df["on_ground_ratio"] = (
            df["on_ground_count"] / df["observation_count"].replace(0, np.nan)
        ).fillna(0)
    else:
        df["on_ground_ratio"] = 0.0

    # velocity_kmh back-fill
    if "avg_velocity_kmh" not in df.columns and "velocity" in df.columns:
        df["avg_velocity_kmh"] = df["velocity"] * 3.6

    # altitude_log (log-scale for better ML representation)
    if "avg_altitude" in df.columns:
        df["altitude_log"] = np.log1p(df["avg_altitude"].clip(lower=0))

    # speed_delta
    if "avg_velocity_kmh" in df.columns and "max_velocity_kmh" in df.columns:
        df["speed_delta"] = (
            df["max_velocity_kmh"].fillna(0) - df["avg_velocity_kmh"].fillna(0)
        ).abs()

    return df


def prepare_X(df: pd.DataFrame, feature_cols: List[str]) -> np.ndarray:
    """Select, impute-with-median, and return feature matrix."""
    available = [c for c in feature_cols if c in df.columns]
    missing   = [c for c in feature_cols if c not in df.columns]
    if missing:
        for c in missing:
            df[c] = 0.0
        available = feature_cols

    X = df[available].copy()
    X = X.fillna(X.median(numeric_only=True))
    return X.values.astype(np.float32)


# ─────────────────────────────────────────────────────────────────────────────
# Model I/O
# ─────────────────────────────────────────────────────────────────────────────
def save_model(model: Any, name: str) -> pathlib.Path:
    path = MODEL_PATH / name
    joblib.dump(model, path)
    logger.info("Saved model → %s", path)
    return path


def load_model(name: str) -> Optional[Any]:
    path = MODEL_PATH / name
    if not path.exists():
        logger.warning("Model file not found: %s", path)
        return None
    model = joblib.load(path)
    logger.info("Loaded model ← %s", path)
    return model


# ─────────────────────────────────────────────────────────────────────────────
# Evaluation helpers
# ─────────────────────────────────────────────────────────────────────────────
def regression_report(y_true: np.ndarray, y_pred: np.ndarray) -> Dict[str, float]:
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
    return {
        "mae":  float(mean_absolute_error(y_true, y_pred)),
        "rmse": float(np.sqrt(mean_squared_error(y_true, y_pred))),
        "r2":   float(r2_score(y_true, y_pred)),
    }


def cluster_report(X: np.ndarray, labels: np.ndarray) -> Dict[str, float]:
    from sklearn.metrics import silhouette_score, davies_bouldin_score
    if len(set(labels)) < 2:
        return {"silhouette": 0.0, "davies_bouldin": 0.0}
    return {
        "silhouette":      float(silhouette_score(X, labels, sample_size=min(1000, len(X)))),
        "davies_bouldin":  float(davies_bouldin_score(X, labels)),
    }
