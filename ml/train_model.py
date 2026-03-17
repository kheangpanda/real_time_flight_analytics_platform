"""
ml/train_model.py
━━━━━━━━━━━━━━━━━
ML training pipeline for the Flight Analytics Platform.

Models trained
─────────────
1. Velocity Predictor      — GradientBoostingRegressor
   Predicts avg_velocity_kmh from altitude, track, vertical_rate

2. Flight Pattern Clusterer — KMeans (k=6)
   Groups flights by speed/altitude profile into travel patterns

3. Anomaly Detector         — IsolationForest
   Flags unusual flights based on speed, altitude, vertical_rate

All models, the feature scaler, and training metadata are saved to
the ./models directory.  Results are also written back to PostgreSQL
main.ml_model_registry for dashboard display.

Usage
─────
  python train_model.py            # train all models
  python train_model.py --model v  # velocity only (v/c/a)
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras

from model_utils import (
    ANOMALY_FEATURES,
    CLUSTER_FEATURES,
    MODEL_PATH,          # single source of truth for where .pkl files live
    TARGET_VELOCITY,
    VELOCITY_FEATURES,
    cluster_report,
    engineer_features,
    prepare_X,
    regression_report,
    save_model,
)

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("ml-trainer")

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────
PG_DSN = (
    f"dbname={os.getenv('POSTGRES_DB', 'flightdw')} "
    f"user={os.getenv('POSTGRES_USER', 'postgres')} "
    f"password={os.getenv('POSTGRES_PASSWORD', 'password')} "
    f"host={os.getenv('POSTGRES_HOST', 'localhost')} "
    f"port={os.getenv('POSTGRES_PORT', '5434')}"
)
MIN_ROWS = int(os.getenv("MIN_TRAINING_ROWS", "100"))
N_CLUSTERS = 6
RANDOM_STATE = 42


# ─────────────────────────────────────────────────────────────────────────────
# Data loading
# ─────────────────────────────────────────────────────────────────────────────
def load_training_data() -> pd.DataFrame:
    logger.info("Loading training data from PostgreSQL…")
    sql = """
        SELECT
            icao24, origin_country,
            avg_velocity_kmh, max_velocity_kmh,
            avg_altitude, max_altitude,
            avg_vertical_rate,
            observation_count, on_ground_count,
            altitude_level, flight_region,
            baro_altitude
        FROM main.flight_analytics
        WHERE avg_velocity_kmh IS NOT NULL
          AND avg_altitude      IS NOT NULL
          AND last_updated >= NOW() - INTERVAL '7 days'
        ORDER BY last_updated DESC
        LIMIT 50000
    """
    try:
        conn = psycopg2.connect(PG_DSN)
        df   = pd.read_sql(sql, conn)
        conn.close()
        logger.info("Loaded %d rows from main.flight_analytics", len(df))
        return df
    except Exception as exc:
        logger.error("DB load failed: %s", exc)
        raise


def _simulate_data(n: int = 2000) -> pd.DataFrame:
    """Generate synthetic data for demo when DB is empty."""
    logger.warning("Generating %d synthetic training rows (DB had insufficient data).", n)
    rng = np.random.default_rng(RANDOM_STATE)

    altitude   = rng.uniform(0, 12000, n)
    velocity   = 150 + altitude * 0.03 + rng.normal(0, 40, n)
    velocity   = np.clip(velocity, 0, 1200)

    return pd.DataFrame({
        "icao24":          [f"SIM{i:05d}" for i in range(n)],
        "origin_country":  rng.choice(["Germany", "USA", "France", "UK", "Japan"], n),
        "avg_velocity_kmh":velocity,
        "max_velocity_kmh":velocity * rng.uniform(1.0, 1.3, n),
        "avg_altitude":    altitude,
        "max_altitude":    altitude * rng.uniform(1.0, 1.15, n),
        "avg_vertical_rate":rng.normal(0, 3, n),
        "observation_count":rng.integers(1, 20, n),
        "on_ground_count": rng.integers(0, 3, n),
        "baro_altitude":   altitude,
        "true_track":      rng.uniform(0, 360, n),
        "altitude_level":  rng.choice(["ground","low","medium","high","very_high"], n),
        "flight_region":   rng.choice(["europe_africa","north_america","asia_pacific"], n),
    })


# ─────────────────────────────────────────────────────────────────────────────
# Registry helpers
# ─────────────────────────────────────────────────────────────────────────────
def register_model(
    name: str, mtype: str, version: str,
    rows: int, metric_name: str, metric_val: float, artifact: str,
) -> None:
    try:
        conn = psycopg2.connect(PG_DSN)
        cur  = conn.cursor()
        cur.execute(
            """
            UPDATE main.ml_model_registry SET is_active = FALSE
            WHERE model_name = %s
            """,
            (name,),
        )
        cur.execute(
            """
            INSERT INTO main.ml_model_registry
              (model_name, model_type, version, training_rows,
               metric_name, metric_value, artifact_path, is_active)
            VALUES (%s,%s,%s,%s,%s,%s,%s,TRUE)
            """,
            (name, mtype, version, rows, metric_name, metric_val, artifact),
        )
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Registered model '%s' in registry.", name)
    except Exception as exc:
        logger.warning("Registry write failed: %s", exc)


# ─────────────────────────────────────────────────────────────────────────────
# 1. Velocity predictor (GBR)
# ─────────────────────────────────────────────────────────────────────────────
def train_velocity_predictor(df: pd.DataFrame) -> Dict:
    from sklearn.ensemble import GradientBoostingRegressor
    from sklearn.model_selection import train_test_split
    from sklearn.preprocessing import StandardScaler

    logger.info("── Training velocity predictor (%d rows)…", len(df))

    df2 = engineer_features(df.copy())

    # We need baro_altitude proxied by avg_altitude if missing
    if "baro_altitude" not in df2.columns:
        df2["baro_altitude"] = df2.get("avg_altitude", 0)
    if "true_track" not in df2.columns:
        df2["true_track"] = 0.0
    if "vertical_rate" not in df2.columns:
        df2["vertical_rate"] = df2.get("avg_vertical_rate", 0)

    feats  = ["baro_altitude", "true_track", "vertical_rate", "on_ground_ratio"]
    target = TARGET_VELOCITY

    valid = df2[feats + [target]].dropna()
    if len(valid) < MIN_ROWS:
        logger.warning("Not enough valid rows (%d). Aborting velocity model.", len(valid))
        return {}

    X = prepare_X(valid, feats)
    y = valid[target].values

    X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.2, random_state=RANDOM_STATE)

    scaler = StandardScaler()
    X_tr_s = scaler.fit_transform(X_tr)
    X_te_s = scaler.transform(X_te)

    model = GradientBoostingRegressor(
        n_estimators=200,
        max_depth=4,
        learning_rate=0.05,
        subsample=0.8,
        random_state=RANDOM_STATE,
    )
    model.fit(X_tr_s, y_tr)

    metrics = regression_report(y_te, model.predict(X_te_s))
    logger.info("Velocity predictor metrics: %s", metrics)

    save_model(model,  "velocity_predictor.pkl")
    save_model(scaler, "feature_scaler.pkl")

    version = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    register_model(
        "velocity_predictor", "regression", version,
        len(valid), "r2", metrics["r2"], "velocity_predictor.pkl",
    )
    return metrics


# ─────────────────────────────────────────────────────────────────────────────
# 2. KMeans flight pattern clustering
# ─────────────────────────────────────────────────────────────────────────────
def train_flight_clusterer(df: pd.DataFrame) -> Dict:
    from sklearn.cluster import KMeans
    from sklearn.preprocessing import StandardScaler

    logger.info("── Training flight clusterer (k=%d, %d rows)…", N_CLUSTERS, len(df))

    df2  = engineer_features(df.copy())
    valid = df2[CLUSTER_FEATURES].dropna()
    if len(valid) < MIN_ROWS:
        logger.warning("Not enough valid rows (%d). Aborting clustering.", len(valid))
        return {}

    X = prepare_X(valid, CLUSTER_FEATURES)

    scaler   = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    model = KMeans(
        n_clusters=N_CLUSTERS,
        n_init=20,
        random_state=RANDOM_STATE,
        max_iter=300,
    )
    model.fit(X_scaled)
    labels = model.labels_

    metrics = cluster_report(X_scaled, labels)
    logger.info("Cluster metrics: %s", metrics)

    # Print cluster centroids in human-readable form
    centres = pd.DataFrame(
        scaler.inverse_transform(model.cluster_centers_),
        columns=CLUSTER_FEATURES,
    )
    logger.info("Cluster centres:\n%s", centres.to_string())

    save_model(model,  "flight_clusterer.pkl")
    save_model(scaler, "cluster_scaler.pkl")

    version = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    register_model(
        "flight_clusterer", "clustering", version,
        len(valid), "silhouette", metrics["silhouette"], "flight_clusterer.pkl",
    )
    return metrics


# ─────────────────────────────────────────────────────────────────────────────
# 3. IsolationForest anomaly detector
# ─────────────────────────────────────────────────────────────────────────────
def train_anomaly_detector(df: pd.DataFrame) -> Dict:
    from sklearn.ensemble import IsolationForest
    from sklearn.preprocessing import StandardScaler

    logger.info("── Training anomaly detector (%d rows)…", len(df))

    df2   = engineer_features(df.copy())
    valid = df2[ANOMALY_FEATURES].dropna()
    if len(valid) < MIN_ROWS:
        logger.warning("Not enough valid rows (%d). Aborting anomaly model.", len(valid))
        return {}

    X = prepare_X(valid, ANOMALY_FEATURES)

    scaler   = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    model = IsolationForest(
        n_estimators=200,
        contamination=0.05,
        random_state=RANDOM_STATE,
        n_jobs=-1,
    )
    model.fit(X_scaled)

    scores  = -model.score_samples(X_scaled)  # higher = more anomalous
    # Normalise to [0,1]
    s_min, s_max = scores.min(), scores.max()
    if s_max > s_min:
        norm_scores = (scores - s_min) / (s_max - s_min)
    else:
        norm_scores = np.zeros_like(scores)

    anomaly_count = int((norm_scores >= 0.7).sum())
    pct           = 100 * anomaly_count / len(norm_scores)
    metrics       = {"anomaly_rate_pct": round(pct, 2)}
    logger.info("Anomaly detector: %.1f%% flagged (≥0.7)", pct)

    save_model(model,  "anomaly_detector.pkl")
    save_model(scaler, "anomaly_scaler.pkl")

    version = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    register_model(
        "anomaly_detector", "anomaly", version,
        len(valid), "anomaly_rate_pct", pct, "anomaly_detector.pkl",
    )
    return metrics


# ─────────────────────────────────────────────────────────────────────────────
# Write cluster labels + anomaly scores back to DB
# ─────────────────────────────────────────────────────────────────────────────
def update_db_with_ml_results(df: pd.DataFrame) -> None:
    """Update main.flight_analytics with cluster_id, anomaly_score, predicted_velocity."""
    try:
        import joblib
        # Use the same MODEL_PATH that save_model() writes to — single source of truth.
        base = MODEL_PATH
        logger.info("Loading models from: %s", base)

        for fname in ["velocity_predictor.pkl", "feature_scaler.pkl",
                      "flight_clusterer.pkl", "anomaly_detector.pkl"]:
            if not (base / fname).exists():
                logger.warning("Skipping DB update — model %s missing.", fname)
                return

        vel_model   = joblib.load(base / "velocity_predictor.pkl")
        vel_scaler  = joblib.load(base / "feature_scaler.pkl")
        clu_model   = joblib.load(base / "flight_clusterer.pkl")
        clu_scaler  = joblib.load(base / "cluster_scaler.pkl")
        ano_model   = joblib.load(base / "anomaly_detector.pkl")
        ano_scaler  = joblib.load(base / "anomaly_scaler.pkl")

        df2 = engineer_features(df.copy())

        # Velocity prediction
        vel_feats = ["baro_altitude", "true_track", "vertical_rate", "on_ground_ratio"]
        if "baro_altitude" not in df2.columns:
            df2["baro_altitude"] = df2.get("avg_altitude", 0)
        if "true_track" not in df2.columns:
            df2["true_track"] = 0.0
        if "vertical_rate" not in df2.columns:
            df2["vertical_rate"] = df2.get("avg_vertical_rate", 0)

        Xv = prepare_X(df2, vel_feats)
        pred_vel = vel_model.predict(vel_scaler.transform(Xv))

        # Clustering
        Xc       = prepare_X(df2, CLUSTER_FEATURES)
        clusters = clu_model.predict(clu_scaler.transform(Xc))

        # Anomaly scores
        Xa         = prepare_X(df2, ANOMALY_FEATURES)
        raw_scores = -ano_model.score_samples(ano_scaler.transform(Xa))
        s_min, s_max = raw_scores.min(), raw_scores.max()
        anomaly_scores = (
            (raw_scores - s_min) / (s_max - s_min)
            if s_max > s_min
            else np.zeros_like(raw_scores)
        )

        # Push back to DB — use enumerate so numpy arrays are addressed by
        # positional index (0..n-1), not by the DataFrame's row label.
        conn    = psycopg2.connect(PG_DSN)
        cur     = conn.cursor()
        updated = 0
        for pos, (_, row) in enumerate(df2.iterrows()):
            cur.execute(
                """
                UPDATE main.flight_analytics
                SET cluster_id         = %s,
                    anomaly_score      = %s,
                    predicted_velocity = %s
                WHERE icao24 = %s
                """,
                (
                    int(clusters[pos]),
                    float(anomaly_scores[pos]),
                    float(pred_vel[pos]),
                    row["icao24"],
                ),
            )
            updated += 1
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Updated %d rows in main.flight_analytics with ML results.", updated)

    except Exception as exc:
        logger.error("ML DB update failed: %s", exc)


# ─────────────────────────────────────────────────────────────────────────────
# CLI entry point
# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="Train flight analytics ML models")
    parser.add_argument(
        "--model",
        choices=["v", "c", "a", "all"],
        default="all",
        help="Which model to train: v=velocity, c=cluster, a=anomaly, all=all",
    )
    args = parser.parse_args()

    start = time.time()
    logger.info("ML training pipeline starting (models=%s)…", args.model)
    logger.info("Model output path: %s", MODEL_PATH)

    try:
        df = load_training_data()
    except Exception:
        df = pd.DataFrame()

    if len(df) < MIN_ROWS:
        df = _simulate_data(5000)

    results = {}

    if args.model in ("v", "all"):
        results["velocity"] = train_velocity_predictor(df)

    if args.model in ("c", "all"):
        results["clustering"] = train_flight_clusterer(df)

    if args.model in ("a", "all"):
        results["anomaly"] = train_anomaly_detector(df)

    if args.model == "all" and len(df) >= MIN_ROWS:
        logger.info("Writing ML results back to PostgreSQL…")
        update_db_with_ml_results(df)

    elapsed = time.time() - start
    logger.info("Training complete in %.1fs. Summary:", elapsed)
    for name, m in results.items():
        logger.info("  %-20s %s", name, json.dumps(m, indent=None))


if __name__ == "__main__":
    main()
