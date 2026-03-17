"""
dashboard/charts.py
Builds Plotly chart figures from query result data — light mode theme.
Each function returns a Plotly Figure serialised to JSON via plotly.io.to_json.
"""
from __future__ import annotations

from typing import Dict, List

import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
import pandas as pd

# ── Light mode palette ────────────────────────────────────────────────────────
TEMPLATE  = "plotly_white"
BG_COLOR  = "#f8fafc"      # page background
PAPER_BG  = "#ffffff"      # chart background
FONT_CLR  = "#1e293b"      # primary text
MUTED_CLR = "#64748b"      # axis labels / legends
GRID_CLR  = "#e2e8f0"      # grid lines
ACCENT    = "#2563eb"      # primary blue
ACCENT2   = "#7c3aed"      # purple accent
BORDER    = "#cbd5e1"


def _fig_json(fig: go.Figure) -> str:
    """Apply common light-mode layout and serialise to JSON."""
    fig.update_layout(
        paper_bgcolor = PAPER_BG,
        plot_bgcolor  = BG_COLOR,
        font          = dict(color=FONT_CLR, family="Inter, system-ui, sans-serif", size=12),
        margin        = dict(l=40, r=20, t=48, b=40),
        title_font    = dict(size=14, color=FONT_CLR),
    )
    fig.update_xaxes(gridcolor=GRID_CLR, linecolor=BORDER, zerolinecolor=GRID_CLR)
    fig.update_yaxes(gridcolor=GRID_CLR, linecolor=BORDER, zerolinecolor=GRID_CLR)
    return pio.to_json(fig)


def empty_chart(title: str = "No data yet") -> str:
    fig = go.Figure()
    fig.add_annotation(
        text="⏳ Waiting for data…",
        xref="paper", yref="paper", x=0.5, y=0.5,
        showarrow=False, font=dict(size=15, color=MUTED_CLR),
    )
    fig.update_layout(title=title, template=TEMPLATE)
    return _fig_json(fig)


# ── Chart 1 — Global Flight Map ───────────────────────────────────────────────
def flight_map_chart(rows: List[Dict]) -> str:
    if not rows:
        return empty_chart("Global Flight Map")

    df = pd.DataFrame(rows).dropna(subset=["longitude", "latitude"])
    if df.empty:
        return empty_chart("Global Flight Map")

    df["callsign"]         = df["callsign"].fillna("N/A")
    df["origin_country"]   = df["origin_country"].fillna("Unknown")
    df["baro_altitude"]    = df["baro_altitude"].fillna(0)
    df["avg_velocity_kmh"] = df["avg_velocity_kmh"].fillna(0)

    fig = px.scatter_geo(
        df,
        lat="latitude", lon="longitude",
        color="avg_velocity_kmh",
        color_continuous_scale=px.colors.sequential.Blues,
        hover_name="callsign",
        hover_data={
            "origin_country":  True,
            "baro_altitude":   ":.0f",
            "avg_velocity_kmh":":.1f",
            "altitude_level":  True,
            "latitude":        False,
            "longitude":       False,
        },
        title="🌍 Global Flight Map",
        template=TEMPLATE,
        size_max=6,
        opacity=0.85,
    )
    fig.update_geos(
        showcoastlines=True,  coastlinecolor="#94a3b8",
        showland=True,        landcolor="#f1f5f9",
        showocean=True,       oceancolor="#dbeafe",
        showlakes=True,       lakecolor="#bfdbfe",
        showframe=False,
        projection_type="natural earth",
    )
    fig.update_coloraxes(colorbar_title="Speed (km/h)",
                         colorbar_tickfont=dict(color=FONT_CLR))
    fig.update_layout(height=420)
    return _fig_json(fig)


# ── Chart 2 — Flights by Country ──────────────────────────────────────────────
def country_bar_chart(rows: List[Dict]) -> str:
    if not rows:
        return empty_chart("Flights by Country")

    df = pd.DataFrame(rows).dropna(subset=["origin_country"])
    df = df.sort_values("flight_count", ascending=True).tail(25)

    fig = px.bar(
        df,
        x="flight_count", y="origin_country",
        orientation="h",
        color="flight_count",
        color_continuous_scale=px.colors.sequential.Blues,
        title="✈️ Active Flights by Country",
        labels={"flight_count": "Flights", "origin_country": "Country"},
        template=TEMPLATE,
    )
    fig.update_traces(marker_line_color=BORDER, marker_line_width=0.5)
    fig.update_layout(showlegend=False, coloraxis_showscale=False,
                      height=420, yaxis_title=None)
    return _fig_json(fig)


# ── Chart 3 — Velocity Distribution ──────────────────────────────────────────
def velocity_histogram(rows: List[Dict]) -> str:
    if not rows:
        return empty_chart("Velocity Distribution")

    df = pd.DataFrame(rows)
    if "velocity_bucket" not in df.columns:
        return empty_chart("Velocity Distribution")

    fig = go.Figure(go.Bar(
        x=df["velocity_bucket"],
        y=df["count"],
        marker=dict(
            color=df["velocity_bucket"],
            colorscale="Blues",
            line=dict(color=PAPER_BG, width=0.8),
        ),
        hovertemplate="Speed: %{x} km/h<br>Flights: %{y}<extra></extra>",
    ))
    fig.update_layout(
        title="🚀 Velocity Distribution (km/h)",
        xaxis_title="Speed bucket (km/h)",
        yaxis_title="Aircraft count",
        template=TEMPLATE,
        height=300,
    )
    return _fig_json(fig)


# ── Chart 4 — Altitude Distribution (donut) ───────────────────────────────────
ALT_COLORS = {
    "ground":    "#ef4444",
    "low":       "#f97316",
    "medium":    "#eab308",
    "high":      "#22c55e",
    "very_high": "#2563eb",
    "unknown":   "#94a3b8",
}


def altitude_donut(rows: List[Dict]) -> str:
    if not rows:
        return empty_chart("Altitude Distribution")

    df     = pd.DataFrame(rows)
    colors = [ALT_COLORS.get(lv, "#94a3b8") for lv in df["altitude_level"]]

    fig = go.Figure(go.Pie(
        labels=df["altitude_level"],
        values=df["count"],
        hole=0.55,
        marker=dict(colors=colors, line=dict(color=PAPER_BG, width=2)),
        hovertemplate="%{label}: %{value} flights (%{percent})<extra></extra>",
        textinfo="label+percent",
        textfont=dict(color=FONT_CLR),
    ))
    fig.update_layout(
        title="🛫 Altitude Distribution",
        template=TEMPLATE, height=300, showlegend=False,
    )
    return _fig_json(fig)


# ── Chart 5 — Flights Over Time ───────────────────────────────────────────────
def time_series_chart(rows: List[Dict]) -> str:
    if not rows:
        return empty_chart("Flights Over Time")

    df = pd.DataFrame(rows)
    df["hour"] = pd.to_datetime(df["hour"])
    df = df.sort_values("hour")

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["hour"], y=df["flight_count"],
        mode="lines+markers",
        name="Flights",
        line=dict(color=ACCENT, width=2),
        marker=dict(size=4, color=ACCENT),
        fill="tozeroy",
        fillcolor="rgba(37,99,235,0.08)",
        hovertemplate="%{x|%Y-%m-%d %H:%M}<br>Flights: %{y}<extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=df["hour"], y=df["avg_velocity"],
        mode="lines",
        name="Avg Speed (km/h)",
        yaxis="y2",
        line=dict(color="#f97316", width=1.5, dash="dot"),
        hovertemplate="%{x|%H:%M}<br>Speed: %{y:.0f} km/h<extra></extra>",
    ))
    fig.update_layout(
        title="📈 Flight Traffic Over Time",
        template=TEMPLATE, height=300,
        xaxis_title="Hour",
        yaxis_title="Active Flights",
        yaxis2=dict(title="Avg Speed (km/h)", overlaying="y", side="right",
                    showgrid=False, tickfont=dict(color="#f97316")),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
                    bgcolor="rgba(255,255,255,0.8)", bordercolor=BORDER, borderwidth=1),
    )
    return _fig_json(fig)


# ── Chart 6 — Aircraft Density Heatmap ────────────────────────────────────────
def density_heatmap(rows: List[Dict]) -> str:
    if not rows:
        return empty_chart("Aircraft Density Heatmap")

    df = pd.DataFrame(rows).dropna(subset=["longitude", "latitude"])
    if df.empty:
        return empty_chart("Aircraft Density Heatmap")

    fig = go.Figure(go.Densitymapbox(
        lat=df["latitude"],
        lon=df["longitude"],
        z=df.get("weight", pd.Series([1] * len(df))).fillna(1),
        radius=8,
        colorscale="Blues",
        opacity=0.70,
        hovertemplate="Lat: %{lat:.2f}<br>Lon: %{lon:.2f}<extra></extra>",
    ))
    fig.update_layout(
        title="🌡️ Aircraft Density Heatmap",
        mapbox_style="carto-positron",   # light map tile
        mapbox=dict(center=dict(lat=30, lon=0), zoom=1.2),
        template=TEMPLATE,
        height=420,
        margin=dict(l=0, r=0, t=44, b=0),
    )
    return _fig_json(fig)


# ── Chart 7 — ML Cluster Scatter ──────────────────────────────────────────────
def cluster_scatter(rows: List[Dict]) -> str:
    if not rows:
        return empty_chart("Flight Pattern Clusters")

    df = pd.DataFrame(rows).dropna(subset=["avg_velocity_kmh", "avg_altitude"])
    if df.empty or "cluster_id" not in df.columns:
        return empty_chart("Flight Pattern Clusters")

    df["cluster_id"] = df["cluster_id"].fillna(-1).astype(int).astype(str)

    fig = px.scatter(
        df,
        x="avg_velocity_kmh", y="avg_altitude",
        color="cluster_id",
        color_discrete_sequence=px.colors.qualitative.Safe,
        hover_name="callsign",
        hover_data={"avg_velocity_kmh": ":.1f", "avg_altitude": ":.0f"},
        title="🤖 Flight Pattern Clusters (ML)",
        labels={"avg_velocity_kmh": "Avg Speed (km/h)", "avg_altitude": "Avg Altitude (m)"},
        template=TEMPLATE,
        opacity=0.7,
    )
    fig.update_layout(height=320, legend_title="Cluster",
                      legend=dict(bgcolor="rgba(255,255,255,0.9)",
                                  bordercolor=BORDER, borderwidth=1))
    return _fig_json(fig)


# ── Chart 8 — Anomaly Score Bar ───────────────────────────────────────────────
def anomaly_chart(rows: List[Dict]) -> str:
    if not rows:
        return empty_chart("Anomaly Alerts")

    df = pd.DataFrame(rows).sort_values("anomaly_score", ascending=False).head(20)

    fig = go.Figure(go.Bar(
        x=df["anomaly_score"],
        y=df["callsign"].fillna(df["icao24"]),
        orientation="h",
        marker=dict(
            color=df["anomaly_score"],
            colorscale="Reds",
            cmin=0, cmax=1,
            line=dict(color=PAPER_BG, width=0.5),
        ),
        hovertemplate="Score: %{x:.3f}<extra></extra>",
    ))
    fig.update_layout(
        title="⚠️ Anomaly Alerts (score ≥ 0.7)",
        xaxis_title="Anomaly Score",
        yaxis_title=None,
        template=TEMPLATE, height=320,
    )
    return _fig_json(fig)


# ── Chart 9 — Region Radar ────────────────────────────────────────────────────
def region_radar(rows: List[Dict]) -> str:
    if not rows:
        return empty_chart("Region Comparison")

    df = pd.DataFrame(rows).fillna(0)
    if df.empty:
        return empty_chart("Region Comparison")

    categories = list(df["flight_region"])
    fig = go.Figure()
    fig.add_trace(go.Scatterpolar(
        r=list(df["flight_count"]),
        theta=categories,
        fill="toself",
        fillcolor="rgba(37,99,235,0.12)",
        name="Flight Count",
        line=dict(color=ACCENT, width=2),
    ))
    fig.update_layout(
        polar=dict(
            bgcolor=PAPER_BG,
            radialaxis=dict(visible=True, gridcolor=GRID_CLR,
                            tickfont=dict(color=MUTED_CLR)),
            angularaxis=dict(gridcolor=GRID_CLR,
                             tickfont=dict(color=FONT_CLR)),
        ),
        title="🗺️ Traffic by Region",
        template=TEMPLATE, height=300, showlegend=False,
    )
    return _fig_json(fig)
