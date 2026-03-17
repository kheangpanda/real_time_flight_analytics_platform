"""
dashboard/db.py
Database access layer for the Flask dashboard.
All queries return plain Python dicts / lists so that
Flask routes can serialise them directly to JSON.
"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any, Dict, Generator, List, Optional

import psycopg2
import psycopg2.extras
import os

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Connection helper
# ─────────────────────────────────────────────────────────────────────────────
def _dsn() -> str:
    return (
        f"dbname={os.getenv('POSTGRES_DB', 'flightdw')} "
        f"user={os.getenv('POSTGRES_USER', 'flightuser')} "
        f"password={os.getenv('POSTGRES_PASSWORD', 'flightpass123')} "
        f"host={os.getenv('POSTGRES_HOST', 'localhost')} "
        f"port={os.getenv('POSTGRES_PORT', '5432')}"
    )


@contextmanager
def get_conn() -> Generator[psycopg2.extensions.connection, None, None]:
    conn = psycopg2.connect(_dsn(), cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def query(sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
    """Execute a SELECT and return rows as a list of dicts."""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return [dict(row) for row in cur.fetchall()]
    except Exception as exc:
        logger.error("DB query failed: %s | SQL: %.200s", exc, sql)
        return []


# ─────────────────────────────────────────────────────────────────────────────
# Dashboard data queries
# ─────────────────────────────────────────────────────────────────────────────

def get_summary_metrics() -> Dict[str, Any]:
    rows = query("""
        SELECT
            COUNT(DISTINCT icao24)                          AS active_flights,
            ROUND(AVG(avg_velocity_kmh)::numeric, 1)        AS avg_speed_kmh,
            ROUND(AVG(avg_altitude)::numeric, 0)            AS avg_altitude_m,
            COUNT(DISTINCT origin_country)                  AS countries,
            SUM(on_ground_count)                            AS on_ground
        FROM main.flight_analytics
        WHERE last_updated >= NOW() - INTERVAL '10 minutes'
    """)
    if rows:
        return dict(rows[0])
    return {
        "active_flights": 0, "avg_speed_kmh": 0,
        "avg_altitude_m": 0, "countries": 0, "on_ground": 0,
    }


def get_flight_map_data(country_filter: Optional[str] = None, limit: int = 2000) -> List[Dict]:
    sql = """
        SELECT
            icao24, callsign, origin_country,
            longitude, latitude, baro_altitude,
            avg_velocity_kmh, altitude_level, flight_region,
            cluster_id, anomaly_score
        FROM main.flight_analytics
        WHERE longitude  IS NOT NULL
          AND latitude   IS NOT NULL
          AND last_updated >= NOW() - INTERVAL '15 minutes'
          {country_clause}
        ORDER BY last_updated DESC
        LIMIT %s
    """
    country_clause = "AND origin_country = %s" if country_filter else ""
    params = (country_filter, limit) if country_filter else (limit,)
    return query(sql.format(country_clause=country_clause), params)


def get_country_bar_data(limit: int = 30) -> List[Dict]:
    return query("""
        SELECT
            origin_country,
            SUM(flight_count)   AS flight_count,
            AVG(avg_altitude)   AS avg_altitude,
            AVG(avg_velocity)   AS avg_velocity
        FROM main.country_traffic_summary
        WHERE snapshot_time >= NOW() - INTERVAL '30 minutes'
          AND origin_country IS NOT NULL
        GROUP BY origin_country
        ORDER BY flight_count DESC
        LIMIT %s
    """, (limit,))


def get_velocity_histogram() -> List[Dict]:
    return query("""
        SELECT
            FLOOR(avg_velocity_kmh / 50) * 50  AS velocity_bucket,
            COUNT(*)                            AS count
        FROM main.flight_analytics
        WHERE avg_velocity_kmh IS NOT NULL
          AND avg_velocity_kmh > 0
          AND last_updated >= NOW() - INTERVAL '30 minutes'
        GROUP BY 1
        ORDER BY 1
    """)


def get_altitude_histogram() -> List[Dict]:
    return query("""
        SELECT
            altitude_level,
            COUNT(*) AS count
        FROM main.flight_analytics
        WHERE altitude_level IS NOT NULL
          AND last_updated >= NOW() - INTERVAL '30 minutes'
        GROUP BY altitude_level
        ORDER BY count DESC
    """)


def get_time_series(hours: int = 24) -> List[Dict]:
    return query("""
        SELECT
            DATE_TRUNC('hour', bucket)  AS hour,
            SUM(flight_count)           AS flight_count,
            AVG(avg_velocity)           AS avg_velocity,
            AVG(avg_altitude)           AS avg_altitude
        FROM main.hourly_flight_counts
        WHERE bucket >= NOW() - INTERVAL '%s hours'
        GROUP BY 1
        ORDER BY 1
    """, (hours,))


def get_heatmap_data(limit: int = 3000) -> List[Dict]:
    return query("""
        SELECT longitude, latitude, avg_velocity_kmh AS weight
        FROM main.flight_analytics
        WHERE longitude  IS NOT NULL
          AND latitude   IS NOT NULL
          AND last_updated >= NOW() - INTERVAL '30 minutes'
        LIMIT %s
    """, (limit,))


def get_ml_cluster_data() -> List[Dict]:
    return query("""
        SELECT
            icao24, callsign, longitude, latitude,
            avg_velocity_kmh, avg_altitude,
            cluster_id, anomaly_score,
            predicted_velocity
        FROM main.flight_analytics
        WHERE cluster_id IS NOT NULL
          AND longitude IS NOT NULL
          AND last_updated >= NOW() - INTERVAL '30 minutes'
        LIMIT 1000
    """)


def get_anomaly_flights(threshold: float = 0.7) -> List[Dict]:
    return query("""
        SELECT
            icao24, callsign, origin_country,
            longitude, latitude, avg_velocity_kmh,
            avg_altitude, anomaly_score
        FROM main.flight_analytics
        WHERE anomaly_score >= %s
          AND last_updated >= NOW() - INTERVAL '30 minutes'
        ORDER BY anomaly_score DESC
        LIMIT 50
    """, (threshold,))


def get_distinct_countries() -> List[str]:
    rows = query("""
        SELECT DISTINCT origin_country
        FROM main.flight_analytics
        WHERE origin_country IS NOT NULL
          AND last_updated >= NOW() - INTERVAL '30 minutes'
        ORDER BY origin_country
    """)
    return [r["origin_country"] for r in rows]


def get_region_summary() -> List[Dict]:
    return query("""
        SELECT
            flight_region,
            COUNT(DISTINCT icao24)        AS flight_count,
            AVG(avg_velocity_kmh)         AS avg_velocity,
            AVG(avg_altitude)             AS avg_altitude
        FROM main.flight_analytics
        WHERE flight_region IS NOT NULL
          AND last_updated >= NOW() - INTERVAL '30 minutes'
        GROUP BY flight_region
        ORDER BY flight_count DESC
    """)
