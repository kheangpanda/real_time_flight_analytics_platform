"""
dashboard/app.py
Flask web server — serves the interactive flight analytics dashboard.

Routes
──────
GET /                   → main dashboard HTML
GET /api/summary        → JSON summary metrics
GET /api/charts         → JSON dict of all Plotly chart specs
GET /api/countries      → JSON list of distinct countries
GET /api/anomalies      → JSON list of anomalous flights
GET /api/ml/predict     → JSON ML velocity predictions (POST)
GET /api/ml/status      → JSON model load status + any errors
GET /api/ml/reload      → force-reload all .pkl files (GET)
GET /api/map_data       → JSON flight positions
GET /health             → liveness probe
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from functools import wraps
from typing import Callable

from flask import Flask, jsonify, render_template, request

import db
import charts

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("flight-dashboard")

# ─────────────────────────────────────────────────────────────────────────────
# App
# ─────────────────────────────────────────────────────────────────────────────
app = Flask(__name__, template_folder="templates", static_folder="static")
app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev-secret")

MODEL_PATH = os.getenv("MODEL_PATH", "./models")
logger.info("MODEL_PATH = %s", MODEL_PATH)

# ─────────────────────────────────────────────────────────────────────────────
# Simple in-memory cache (TTL-based)
# ─────────────────────────────────────────────────────────────────────────────
_cache: dict = {}


def _cached(key: str, ttl: int = 10) -> Callable:
    def decorator(fn: Callable) -> Callable:
        @wraps(fn)
        def wrapper(*args, **kwargs):
            now = time.time()
            if key in _cache and (now - _cache[key]["ts"]) < ttl:
                return _cache[key]["val"]
            result = fn(*args, **kwargs)
            _cache[key] = {"val": result, "ts": now}
            return result
        return wrapper
    return decorator


# ─────────────────────────────────────────────────────────────────────────────
# ML model loader
# Each .pkl is loaded independently — one bad file never blocks the others.
# The four "core" models must all be present before predictions are served.
# ─────────────────────────────────────────────────────────────────────────────
_ml_models: dict = {}
_ml_errors: dict = {}   # fname → error string, shown in /api/ml/status

_MODEL_FILES = [
    ("velocity_predictor", "velocity_predictor.pkl"),
    ("scaler",             "feature_scaler.pkl"),
    ("flight_clusterer",   "flight_clusterer.pkl"),
    ("anomaly_detector",   "anomaly_detector.pkl"),
]
_CORE_MODELS = {"velocity_predictor", "scaler"}


def _load_ml_models(force: bool = False) -> dict:
    """
    Attempt to load every .pkl independently.
    Set force=True to discard the current cache and reload from disk.
    Returns the dict of successfully loaded models.
    """
    global _ml_models, _ml_errors

    if force:
        _ml_models = {}
        _ml_errors = {}

    # Skip if all core models are already loaded
    if _ml_models.keys() >= _CORE_MODELS:
        return _ml_models

    import joblib
    import pathlib

    base = pathlib.Path(MODEL_PATH)
    logger.info("Loading ML models from: %s  (exists=%s)", base, base.exists())

    for name, fname in _MODEL_FILES:
        if name in _ml_models:          # already loaded
            continue
        path = base / fname
        if not path.exists():
            msg = f"file not found: {path}"
            _ml_errors[name] = msg
            logger.warning("ML model missing — %s", msg)
            continue
        try:
            _ml_models[name] = joblib.load(path)
            _ml_errors.pop(name, None)
            logger.info("✅ Loaded: %s", fname)
        except Exception as exc:
            _ml_errors[name] = str(exc)
            logger.error("❌ Failed to load %s: %s", fname, exc)

    if _ml_models.keys() >= _CORE_MODELS:
        logger.info("All core ML models ready.")
    else:
        missing = _CORE_MODELS - _ml_models.keys()
        logger.warning("Core models not yet available: %s", missing)

    return _ml_models


# ─────────────────────────────────────────────────────────────────────────────
# Routes
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/health")
def health():
    return jsonify({"status": "ok", "ts": datetime.now(timezone.utc).isoformat()})


@app.route("/")
def dashboard():
    return render_template(
        "dashboard.html",
        refresh_ms=30_000,
        build_time=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
    )


@app.route("/api/summary")
@_cached("summary", ttl=10)
def api_summary():
    return jsonify(db.get_summary_metrics())


@app.route("/api/charts")
def api_charts():
    country = request.args.get("country") or None
    time_h  = int(request.args.get("hours", 24))

    payload = {
        "flight_map":    charts.flight_map_chart(db.get_flight_map_data(country_filter=country)),
        "country_bar":   charts.country_bar_chart(db.get_country_bar_data()),
        "velocity_hist": charts.velocity_histogram(db.get_velocity_histogram()),
        "altitude_dist": charts.altitude_donut(db.get_altitude_histogram()),
        "time_series":   charts.time_series_chart(db.get_time_series(hours=time_h)),
        "density_heat":  charts.density_heatmap(db.get_heatmap_data()),
        "cluster_plot":  charts.cluster_scatter(db.get_ml_cluster_data()),
        "anomaly_bar":   charts.anomaly_chart(db.get_anomaly_flights()),
        "region_radar":  charts.region_radar(db.get_region_summary()),
        "generated_at":  datetime.now(timezone.utc).isoformat(),
    }
    return jsonify(payload)


@app.route("/api/countries")
@_cached("countries", ttl=60)
def api_countries():
    return jsonify(db.get_distinct_countries())


@app.route("/api/anomalies")
def api_anomalies():
    threshold = float(request.args.get("threshold", 0.7))
    return jsonify(db.get_anomaly_flights(threshold=threshold))


@app.route("/api/ml/status")
def api_ml_status():
    """Diagnostic endpoint — shows which models are loaded and any errors."""
    models  = _load_ml_models()
    import pathlib
    base    = pathlib.Path(MODEL_PATH)
    files   = {}
    for name, fname in _MODEL_FILES:
        p = base / fname
        files[fname] = {
            "exists":  p.exists(),
            "loaded":  name in models,
            "error":   _ml_errors.get(name),
            "size_kb": round(p.stat().st_size / 1024, 1) if p.exists() else None,
        }
    return jsonify({
        "model_path":     str(base),
        "path_exists":    base.exists(),
        "models_loaded":  list(models.keys()),
        "core_ready":     models.keys() >= _CORE_MODELS,
        "files":          files,
        "errors":         _ml_errors,
    })


@app.route("/api/ml/reload")
def api_ml_reload():
    """Force-discard the model cache and reload all .pkl files from disk."""
    models = _load_ml_models(force=True)
    return jsonify({
        "reloaded":    list(models.keys()),
        "errors":      _ml_errors,
        "core_ready":  models.keys() >= _CORE_MODELS,
    })


@app.route("/api/ml/predict", methods=["POST"])
def api_ml_predict():
    """
    POST {"features": [[alt, vr, on_ground_ratio, true_track], ...]}
    Returns predicted velocities (km/h).
    """
    models = _load_ml_models()
    if not (models.keys() >= _CORE_MODELS):
        missing = list(_CORE_MODELS - models.keys())
        errors  = {k: v for k, v in _ml_errors.items() if k in _CORE_MODELS}
        return jsonify({
            "error":   "Core ML models not loaded.",
            "missing": missing,
            "details": errors,
            "hint":    f"Check /api/ml/status — model path is {MODEL_PATH}",
        }), 503

    try:
        import numpy as np
        body     = request.get_json(force=True)
        features = body.get("features", [])
        X        = np.array(features, dtype=float)
        X_scaled = models["scaler"].transform(X)
        preds    = models["velocity_predictor"].predict(X_scaled).tolist()
        return jsonify({"predictions": preds, "count": len(preds)})
    except Exception as exc:
        logger.error("Prediction error: %s", exc)
        return jsonify({"error": str(exc)}), 400


@app.route("/api/map_data")
def api_map_data():
    country = request.args.get("country") or None
    return jsonify(db.get_flight_map_data(country_filter=country))


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    _load_ml_models()   # eagerly try on startup so logs appear immediately
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)
