from __future__ import annotations

import json
import math
from html import escape
from pathlib import Path
from urllib.parse import urlparse

import altair as alt
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components


APP_DIR = Path(__file__).resolve().parent
DATA_DIR = APP_DIR / "data"
PORTLAND_DATA_DIR = DATA_DIR / "portland"
PORTLAND_CHORD_DIR = PORTLAND_DATA_DIR / "topic_chords" / "portland_all_topic_chords_v001"
CHORD_TEMPLATE_PATH = APP_DIR / "grouped_chord_component.html"
DATA_VERSION = "synthetic_bar_attention_shares_v1"
PORTLAND_DATA_VERSION = "portland_dashboard_packet_v007_prepared_v1"
CHORD_DEFAULT_INTERSECTIONS = 12

COLORS = {
    "orange": "#ef5b21",
    "teal": "#0d7685",
    "blue": "#1f68d5",
    "purple": "#5a36b8",
    "green": "#6aa56f",
    "slate": "#243247",
    "muted": "#667085",
}
OVERVIEW_RANK_COLORS = [
    COLORS["orange"],
    COLORS["green"],
    COLORS["blue"],
    COLORS["purple"],
    COLORS["teal"],
    "#b7791f",
]

OVERVIEW_CHART_HEIGHT = 345
ONTOLOGY_CHART_HEIGHT = 590
OVERVIEW_SUMMARY_MIN_HEIGHT = 410
OVERVIEW_BAR_LIST_HEIGHT = 315
ONTOLOGY_DOMAINS = [
    ("health", "Health"),
    ("environmental_health", "Environmental exposures"),
    ("social_structural", "Social and structural conditions"),
]
ONTOLOGY_COLLAPSE_STATE_VERSION = "portland_domains_v1"


st.set_page_config(
    page_title="Children's Environmental Health Topics",
    layout="wide",
    initial_sidebar_state="collapsed",
)


@st.cache_data
def load_data(data_version: str = DATA_VERSION) -> dict[str, pd.DataFrame]:
    _ = data_version
    return {
        "timeseries": pd.read_parquet(DATA_DIR / "topic_timeseries.parquet"),
        "events": pd.read_parquet(DATA_DIR / "candidate_events.parquet"),
        "ontology": pd.read_parquet(DATA_DIR / "ontology_index.parquet"),
        "intersections": pd.read_parquet(DATA_DIR / "intersections.parquet"),
        "label_summary": pd.read_parquet(DATA_DIR / "label_summary.parquet"),
        "overview_summary": pd.read_parquet(DATA_DIR / "overview_summary.parquet"),
        "summaries": pd.read_parquet(DATA_DIR / "generated_summaries.parquet"),
    }


@st.cache_data
def load_portland_data(data_version: str = PORTLAND_DATA_VERSION) -> dict[str, pd.DataFrame]:
    _ = data_version
    return {
        "timeseries": pd.read_parquet(PORTLAND_DATA_DIR / "topic_timeseries.parquet"),
        "events": pd.read_parquet(PORTLAND_DATA_DIR / "candidate_events.parquet"),
        "categories": pd.read_parquet(PORTLAND_DATA_DIR / "categories.parquet"),
        "label_summary": pd.read_parquet(PORTLAND_DATA_DIR / "label_summary.parquet"),
        "overview_summary": pd.read_parquet(PORTLAND_DATA_DIR / "overview_summary.parquet"),
        "topic_label_scores": pd.read_parquet(PORTLAND_DATA_DIR / "topic_label_scores.parquet"),
        "summaries": pd.read_parquet(PORTLAND_DATA_DIR / "generated_summaries.parquet"),
    }


def default_collapsed_ontology() -> set[str]:
    return {f"domain::{topic_group}" for topic_group, _ in ONTOLOGY_DOMAINS}


def default_portland_label(data: dict[str, pd.DataFrame] | None = None) -> str:
    data = data or load_portland_data(PORTLAND_DATA_VERSION)
    rows = data["overview_summary"].sort_values(
        ["topic_group", "attention_share_pct"],
        ascending=[True, False],
    )
    health = rows[rows["topic_group"].eq("health")]
    if not health.empty:
        return str(health.iloc[0]["label"])
    return str(rows.iloc[0]["label"])


def domain_selection_value(topic_group: str) -> str:
    return f"domain::{topic_group}"


def domain_from_selection(selection: str) -> str | None:
    if not selection.startswith("domain::"):
        return None
    topic_group = selection.removeprefix("domain::")
    return topic_group if topic_group in dict(ONTOLOGY_DOMAINS) else None


def inject_css() -> None:
    st.markdown(
        """
        <style>
        .block-container {
            padding: 1.25rem 1.45rem 0.75rem;
            max-width: 100%;
        }
        header[data-testid="stHeader"] { display: none; }
        .stApp {
            background: #f8fafb;
            color: #142033;
        }
        h1 {
            font-size: 2.35rem !important;
            letter-spacing: 0 !important;
            margin: 0 !important;
            color: #0f172a;
        }
        h2, h3 {
            letter-spacing: 0 !important;
            color: #111827;
        }
        .connection-panel-title {
            color: #111827;
            font-size: 1.5rem;
            font-weight: 800;
            line-height: 1.2;
            margin: 0 0 0.65rem 0;
        }
        .connections-slider-label {
            color: #667085;
            font-size: 0.82rem;
            font-weight: 650;
            line-height: 1.15;
            margin: 0;
            text-align: right;
            transform: translateY(-0.22rem);
        }
        div[data-testid="stVerticalBlock"] { gap: 0.7rem; }
        .panel {
            background: #f3f6f8;
            border: 1px solid #d9e2ec;
            border-radius: 8px;
            padding: 1rem 1.1rem;
            box-shadow: 0 1px 3px rgba(15, 23, 42, 0.03);
        }
        .panel.disabled {
            color: #9aa5b5;
            background: linear-gradient(180deg, #f4f6f8 0%, #edf1f5 100%);
            border-color: #e2e8f0;
            box-shadow: none;
        }
        .panel.disabled h3,
        .panel.disabled .muted,
        .panel.disabled .tree-row,
        .panel.disabled .chev {
            color: #98a2b3 !important;
        }
        .panel.disabled .tree-row.selected {
            color: #98a2b3 !important;
            font-weight: 650;
        }
        .panel.disabled .dot,
        .panel.disabled .box {
            border-color: #b8c2cf !important;
            background: #d5dce5 !important;
        }
        .small-meta {
            color: #0b3b7a;
            font-size: 0.88rem;
            font-weight: 600;
            margin: -0.1rem 0 0.45rem;
        }
        .muted {
            color: #667085;
            font-size: 0.86rem;
        }
        .chart-title {
            color: #111827;
            font-size: 1.05rem;
            font-weight: 800;
            line-height: 1.2;
            margin: 0 1rem 0.3rem;
        }
        .chart-meta {
            color: #0b3b7a;
            font-size: 0.78rem;
            font-weight: 650;
            line-height: 1.2;
            margin: 0.18rem 1rem 0.8rem;
        }
        div[data-testid="stMarkdownContainer"] .chart-title {
            margin-block-start: 0;
            margin-block-end: 0.3rem;
        }
        div[data-testid="stMarkdownContainer"] .chart-meta {
            margin-block-start: 0.18rem;
            margin-block-end: 0.8rem;
        }
        div[data-testid="stColumn"]:has([data-testid="stVegaLiteChart"]) > div[data-testid="stVerticalBlock"] {
            background: #f3f6f8;
            border: 1px solid #d9e2ec;
            border-radius: 8px;
            padding: 0.25rem 0 0.15rem;
            box-shadow: 0 1px 3px rgba(15, 23, 42, 0.03);
        }
        div[data-testid="stColumn"]:has([data-testid="stVegaLiteChart"]) [data-testid="stVegaLiteChart"] {
            margin-top: 0.25rem;
        }
        .tree-row {
            display: flex;
            align-items: center;
            gap: 0.3rem;
            min-height: 1.35rem;
            color: #17233a;
            font-size: 0.88rem;
            white-space: nowrap;
        }
        .tree-row.dim {
            font-weight: 700;
            margin-top: 0.35rem;
        }
        .tree-row.parent {
            font-weight: 650;
            margin-left: 0;
        }
        .tree-row.label {
            margin-left: 0;
            color: #21324d;
        }
        .tree-row.selected {
            color: #ef5b21;
            font-weight: 800;
        }
        .tree-link {
            display: block;
            text-decoration: none !important;
            border-radius: 6px;
        }
        .tree-link:hover {
            background: #fff1e8;
        }
        .tree-toggle {
            color: #17233a !important;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            text-decoration: none !important;
        }
        .tree-toggle:hover {
            color: #ef5b21 !important;
        }
        .control-label {
            color: #0b2b63;
            font-size: 0.88rem;
            font-weight: 700;
            margin: 0 0 0.35rem 0;
            line-height: 1.25;
        }
        .box {
            width: 0.62rem;
            height: 0.62rem;
            border: 1px solid #97a6ba;
            border-radius: 3px;
            display: inline-block;
        }
        .dot {
            width: 0.48rem;
            height: 0.48rem;
            border-radius: 999px;
            background: #294566;
            display: inline-block;
        }
        .dot.orange { background: #ef5b21; }
        .dot.teal { background: #0d7685; }
        .dot.blue { background: #1f68d5; }
        .dot.purple { background: #5a36b8; }
        .chev {
            color: #0d315f;
            font-weight: 800;
            width: 0.75rem;
        }
        .intersection-grid {
            display: grid;
            grid-template-columns: 1fr 1fr 1fr;
            gap: 0.55rem;
        }
        .top-label-grid {
            display: grid;
            grid-template-columns: 1fr 1fr 1fr;
            gap: 0.55rem;
        }
        .mini-card {
            border: 1px solid #dce5ef;
            border-radius: 8px;
            padding: 0.7rem 0.72rem;
            min-height: 9.5rem;
        }
        .mini-title {
            font-size: 0.9rem;
            font-weight: 800;
            color: #0d7685;
            border-bottom: 2px solid #0d7685;
            padding-bottom: 0.4rem;
            margin-bottom: 0.62rem;
            text-align: center;
        }
        .mini-title.blue {
            color: #1f68d5;
            border-color: #1f68d5;
        }
        .mini-title.purple {
            color: #5a36b8;
            border-color: #5a36b8;
        }
        .mini-title.black {
            color: #111827;
            border-color: #111827;
        }
        .bar-row {
            margin-bottom: 0.72rem;
        }
        .bar-row-top {
            display: flex;
            justify-content: space-between;
            gap: 0.5rem;
            font-size: 0.79rem;
            color: #17233a;
            line-height: 1.05rem;
        }
        .bar-track {
            height: 0.38rem;
            border-radius: 999px;
            background: #e9eef3;
            margin-top: 0.28rem;
            overflow: hidden;
        }
        .bar-fill {
            height: 100%;
            border-radius: 999px;
        }
        .metric-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 0.7rem;
        }
        .metric-card {
            border: 1px solid #dce5ef;
            border-radius: 8px;
            background: #f3f6f8;
            min-height: 5.4rem;
            padding: 0.86rem;
            text-align: center;
        }
        .metric-label {
            color: #182a45;
            font-size: 0.88rem;
            font-weight: 700;
        }
        .metric-value {
            color: #ef5b21;
            font-size: 1.8rem;
            font-weight: 850;
            margin-top: 0.12rem;
        }
        .metric-note {
            display: none;
        }
        .summary-text {
            color: #0d2250;
            font-size: 0.95rem;
            line-height: 1.48;
            margin-top: 0.6rem;
        }
        .summary-text p {
            margin: 0;
        }
        .source-links {
            margin: 0.65rem 0 0 1.05rem;
            padding: 0;
        }
        .source-links li {
            margin: 0.2rem 0;
        }
        .source-links a {
            color: #0b3b7a !important;
            font-weight: 650;
            text-decoration: none;
        }
        .source-links a:hover {
            text-decoration: underline;
        }
        .rank-table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.88rem;
            color: #132440;
        }
        .rank-table th {
            text-align: left;
            border-bottom: 1px solid #dce5ef;
            padding: 0.48rem;
            color: #0b3b7a;
        }
        .rank-table td {
            border-bottom: 1px solid #e8eef4;
            padding: 0.56rem 0.48rem;
        }
        .overview-profile-stack {
            display: grid;
            gap: 0.7rem;
        }
        .overview-profile-panel {
            background: #f3f6f8;
            border: 1px solid #d9e2ec;
            border-radius: 8px;
            min-height: 409px;
            padding: 0.9rem 0.95rem;
            box-shadow: 0 1px 3px rgba(15, 23, 42, 0.03);
        }
        div[data-testid="stColumn"]:has(.overview-profile-panel-marker) > div[data-testid="stVerticalBlock"] {
            background: #f3f6f8;
            border: 1px solid #d9e2ec;
            border-radius: 8px;
            min-height: 409px;
            padding: 1rem 1.1rem;
            box-shadow: 0 1px 3px rgba(15, 23, 42, 0.03);
        }
        div[data-testid="stColumn"]:has(.connections-left-panel-marker) > div[data-testid="stVerticalBlock"],
        div[data-testid="stColumn"]:has(.connections-center-panel-marker) > div[data-testid="stVerticalBlock"] {
            background: #f3f6f8;
            border: 1px solid #d9e2ec;
            border-radius: 8px;
            padding: 1rem 1.1rem;
            box-shadow: 0 1px 3px rgba(15, 23, 42, 0.03);
        }
        .overview-profile-panel-marker,
        .connections-left-panel-marker,
        .connections-center-panel-marker {
            display: none;
        }
        .overview-profile-panel h3 {
            margin: 0 0 0.18rem 0;
            line-height: 1.15;
        }
        .overview-profile-title {
            margin: 0 0 0.85rem 0;
            line-height: 1.15;
        }
        .overview-profile-meta {
            color: #667085;
            font-size: 0.78rem;
            line-height: 1.25;
            margin-bottom: 0.72rem;
        }
        .overview-profile-bars {
            display: grid;
            gap: 0.72rem;
        }
        .overview-profile-row {
            margin-bottom: 0;
        }
        .overview-profile-row-top {
            display: flex;
            justify-content: space-between;
            gap: 0.5rem;
            color: #17233a;
            font-size: 0.79rem;
            line-height: 1.05rem;
        }
        .overview-profile-label {
            color: #17233a;
            font-weight: 400;
            min-width: 0;
            overflow-wrap: break-word;
        }
        .overview-profile-track {
            background: #e9eef3;
            border-radius: 999px;
            height: 0.38rem;
            margin-top: -0.1rem;
            margin-bottom: 0.35rem;
            overflow: hidden;
        }
        .overview-profile-fill {
            border-radius: 999px;
            height: 100%;
        }
        .overview-profile-value {
            color: #132440;
            font-weight: 400;
            line-height: 1.05rem;
            text-align: right;
            transform: translateY(-0.6rem);
            white-space: nowrap;
        }
        .overview-profile-row-muted {
            opacity: 0.42;
        }
        div[data-testid="stColumn"]:has(.overview-profile-panel-marker) div[data-testid="stButton"] button {
            align-items: center !important;
            background: transparent !important;
            border: 0 !important;
            box-shadow: none !important;
            color: #17233a !important;
            display: inline-flex !important;
            font-size: 0.79rem !important;
            font-weight: 400 !important;
            justify-content: flex-start !important;
            line-height: 1.05rem !important;
            min-height: 1.35rem !important;
            padding: 0 !important;
            text-align: left !important;
            transform: translateY(0.35rem);
            width: auto !important;
        }
        div[data-testid="stColumn"]:has(.overview-profile-panel-marker) div[data-testid="stButton"] button:hover,
        div[data-testid="stColumn"]:has(.overview-profile-panel-marker) div[data-testid="stButton"] button:focus,
        div[data-testid="stColumn"]:has(.overview-profile-panel-marker) div[data-testid="stButton"] button:active {
            background: transparent !important;
            border: 0 !important;
            box-shadow: none !important;
            color: #ef5b21 !important;
        }
        div[data-testid="stColumn"]:has(.overview-profile-panel-marker) div[data-testid="stButton"] button p {
            color: inherit !important;
            font-size: inherit !important;
            font-weight: inherit !important;
            line-height: inherit !important;
            margin: 0 !important;
        }
        .footer {
            display: flex;
            gap: 0.85rem;
            align-items: flex-start;
            color: #0d2250;
            border-top: 1px solid #d9e2ec;
            padding: 0.78rem 0.2rem 0;
            font-size: 0.84rem;
            line-height: 1.45;
        }
        .shield {
            width: 2.1rem;
            height: 2.1rem;
            border: 2px solid #52657d;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: 900;
            color: #52657d;
            flex: 0 0 auto;
        }
        button[kind="secondary"] {
            border-radius: 8px !important;
        }
        div[data-testid="stButton"] > button {
            align-items: center !important;
            background: transparent !important;
            border: 0 !important;
            border-radius: 6px !important;
            box-shadow: none !important;
            color: #17233a !important;
            display: inline-flex !important;
            font-size: 0.88rem !important;
            font-weight: 650 !important;
            justify-content: flex-start !important;
            line-height: 1.2 !important;
            min-height: 1.35rem !important;
            padding: 0.12rem 0.2rem !important;
            text-align: left !important;
            width: auto !important;
        }
        div[data-testid="stButton"] > button:hover {
            background: #fff1e8 !important;
            color: #ef5b21 !important;
        }
        div[data-testid="stButton"] > button p {
            color: inherit !important;
            font-size: inherit !important;
            font-weight: inherit !important;
            line-height: inherit !important;
            margin: 0 !important;
        }
        div[data-testid="stSegmentedControl"] {
            width: 100%;
        }
        div[data-testid="stSegmentedControl"] label {
            color: #0b2b63 !important;
            font-size: 0.88rem !important;
            font-weight: 700 !important;
        }
        .stApp button[data-variant="segmented_control"] {
            background: #eef2f6 !important;
            border: 1px solid #cbd8e6 !important;
            color: #667085 !important;
            justify-content: center !important;
            min-height: 2.75rem !important;
            width: 100% !important;
        }
        .stApp button[data-variant="segmented_control"][aria-checked="true"],
        .stApp button[data-variant="segmented_control"][data-selected="true"] {
            background: #111827 !important;
            border-color: #111827 !important;
            color: #ffffff !important;
        }
        .stApp button[data-variant="segmented_control"]:hover {
            background: #e2e8f0 !important;
            color: #344054 !important;
        }
        .stApp button[data-variant="segmented_control"][aria-checked="true"]:hover,
        .stApp button[data-variant="segmented_control"][data-selected="true"]:hover {
            background: #111827 !important;
            color: #ffffff !important;
        }
        .stApp button[data-variant="segmented_control"] p {
            color: inherit !important;
            font-weight: 800 !important;
            margin: 0 !important;
        }
        div[data-testid="stToggle"] {
            align-items: center;
            display: flex;
            justify-content: flex-end;
            min-height: 2rem;
        }
        div[data-testid="stToggle"] label {
            color: #17233a !important;
            font-size: 0.82rem !important;
            font-weight: 700 !important;
            gap: 0.45rem !important;
            opacity: 1 !important;
        }
        div[data-testid="stToggle"] [role="switch"] {
            background-color: #d9e2ec !important;
            border: 1px solid #8ea0b7 !important;
            box-shadow: inset 0 0 0 1px rgba(15, 23, 42, 0.08) !important;
            opacity: 1 !important;
        }
        div[data-testid="stToggle"] [role="switch"][aria-checked="true"] {
            background-color: #111827 !important;
            border-color: #111827 !important;
        }
        div[data-testid="stToggle"] [role="switch"] * {
            opacity: 1 !important;
        }
        div[data-testid="stToggle"] [role="switch"][aria-checked="false"] * {
            background-color: #ffffff !important;
        }
        div[data-testid="stSelectbox"] label {
            color: #0b2b63 !important;
            font-size: 0.88rem !important;
            font-weight: 700 !important;
            margin-bottom: 0.15rem !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def label_color(label: str) -> str:
    return {
        "wildfire smoke": COLORS["orange"],
        "air pollution": COLORS["teal"],
        "lead": COLORS["purple"],
        "indoor air quality": COLORS["blue"],
        "heat": "#b7791f",
        "mold": COLORS["green"],
        "asthma": COLORS["orange"],
        "respiratory illness": COLORS["teal"],
        "mental health": COLORS["blue"],
        "wheezing": COLORS["purple"],
        "cough": COLORS["green"],
        "heat illness": "#b7791f",
    }.get(label, COLORS["teal"])


def overview_color_map(group: str, data: dict[str, pd.DataFrame] | None = None) -> dict[str, str]:
    rows = overview_topic_rows(group, limit=None, data=data)
    return {
        row.label: OVERVIEW_RANK_COLORS[index % len(OVERVIEW_RANK_COLORS)]
        for index, row in enumerate(rows.itertuples(index=False))
    }


def default_link_title(url: str) -> str:
    hostname = urlparse(url).netloc.replace("www.", "")
    if not hostname:
        return "Source"
    return hostname.split(".")[0].title()


def event_records_for_labels(events: pd.DataFrame, labels: list[str]) -> pd.DataFrame:
    records = events[events["label"].isin(labels)].copy()
    if records.empty:
        return records
    if "event_url_title" not in records.columns:
        records["event_url_title"] = records["event_url"].map(default_link_title)
    records["start"] = pd.to_datetime(records["start"])
    records["end"] = pd.to_datetime(records["end"])
    records["legend_label"] = records["label"]
    return records


def filter_local_events(events: pd.DataFrame, local_only: bool) -> pd.DataFrame:
    if not local_only or "local_impact_mentioned" not in events.columns:
        return events
    return events[events["local_impact_mentioned"].fillna(False)].copy()


def local_events_only_control(key: str) -> bool:
    if key not in st.session_state:
        st.session_state[key] = "local events only"
    elif isinstance(st.session_state[key], bool):
        st.session_state[key] = "local events only" if st.session_state[key] else "all events"
    elif st.session_state[key] not in {"local events only", "all events"}:
        st.session_state[key] = "local events only"
    selected = st.segmented_control(
        "Events",
        ["local events only", "all events"],
        key=key,
        selection_mode="single",
        required=True,
        width="stretch",
    )
    return selected == "local events only"


def line_chart(
    df: pd.DataFrame,
    title: str,
    labels: list[str],
    events: pd.DataFrame | None = None,
    event_labels: list[str] | None = None,
    height: int = 255,
    attention_column: str | None = None,
    attention_label: str | None = None,
    value_column: str = "rank_percentile",
    y_axis_title: str = "Topic rank percentile",
    color_map: dict[str, str] | None = None,
    event_selection_name: str | None = None,
) -> alt.Chart:
    plot = df[df["label"].isin(labels)].copy()
    if plot.empty and labels:
        fallback = df[df["label"] == "wildfire smoke"].copy()
        fallback["label"] = labels[0]
        plot = fallback
    if not plot.empty:
        plot["month"] = pd.to_datetime(plot["month"])
        plot["legend_label"] = plot["label"]
    color_map = color_map or {}
    color_for = lambda label: color_map.get(label, label_color(label))
    if value_column not in plot.columns:
        value_column = "rank_percentile"
    tooltip_fields = [
        alt.Tooltip("month:T", title="Month", format="%b %Y"),
        alt.Tooltip("label:N", title="Topic"),
        alt.Tooltip(f"{value_column}:Q", title="Topic value"),
        alt.Tooltip("rank_percentile:Q", title="Rank percentile"),
    ]
    if attention_column is not None and attention_column in plot.columns:
        tooltip_fields.append(alt.Tooltip(f"{attention_column}:Q", title="Attention %"))
    attention_data = pd.DataFrame()
    if attention_column is not None and attention_label is not None and attention_column in df.columns:
        attention_data = (
            df[["month", attention_column]]
            .dropna(subset=[attention_column])
            .drop_duplicates()
            .rename(columns={attention_column: "attention_percent"})
            .copy()
        )
        attention_data["month"] = pd.to_datetime(attention_data["month"])
        attention_data["legend_label"] = attention_label
    event_labels = event_labels or labels
    event_data = event_records_for_labels(events, event_labels) if events is not None else pd.DataFrame()
    extra_event_labels = [label for label in event_labels if label not in labels]
    legend_domain = labels + extra_event_labels + ([attention_label] if not attention_data.empty else [])
    legend_range = [COLORS["slate"] if label == attention_label else color_for(label) for label in legend_domain]
    legend_scale = alt.Scale(domain=legend_domain, range=legend_range)

    chart_layers = []
    if not plot.empty:
        lines = (
            alt.Chart(plot)
            .mark_line(strokeWidth=2)
            .encode(
                x=alt.X("month:T", title=None, axis=alt.Axis(format="%Y", tickCount="year")),
                y=alt.Y(f"{value_column}:Q", title=y_axis_title, scale=alt.Scale(domain=[0, 100])),
                color=alt.Color("legend_label:N", title=None, scale=legend_scale, legend=None),
                tooltip=tooltip_fields,
            )
        )
        chart_layers.append(lines)
    has_attention_legend = False
    if not attention_data.empty:
        attention_line = (
            alt.Chart(attention_data)
            .mark_line(strokeWidth=3, strokeDash=[6, 4])
            .encode(
                x=alt.X("month:T", title=None),
                y=alt.Y("attention_percent:Q", title=y_axis_title, scale=alt.Scale(domain=[0, 100])),
                color=alt.Color(
                    "legend_label:N",
                    title=None,
                    scale=alt.Scale(domain=[attention_label], range=[COLORS["slate"]]),
                    legend=alt.Legend(symbolType="stroke", symbolStrokeWidth=3),
                ),
                tooltip=[
                    alt.Tooltip("month:T", title="Month", format="%b %Y"),
                    alt.Tooltip("legend_label:N", title="Metric"),
                    alt.Tooltip("attention_percent:Q", title="Attention %"),
                ],
            )
        )
        chart_layers.append(attention_line)
        has_attention_legend = True
    if not event_data.empty:
        event_selection = (
            alt.selection_point(
                fields=["event_id"],
                name=event_selection_name,
                on="click",
                clear="click[!event.item]",
                empty=False,
            )
            if event_selection_name
            else None
        )
        event_hover_name = f"{event_selection_name or 'event'}_hover"
        event_hover = alt.selection_point(
            fields=["event_id"],
            name=event_hover_name,
            on="pointerover",
            clear="pointerout",
            empty=False,
        )
        event_tooltips = [
            alt.Tooltip("event_name:N", title=""),
        ]
        event_encoding = {
            "x": "start:T",
            "x2": "end:T",
            "color": alt.Color("label:N", title=None, scale=legend_scale, legend=None),
            "tooltip": event_tooltips,
        }
        bands = (
            alt.Chart(event_data)
            .mark_rect(cursor="pointer", strokeWidth=1.5)
            .encode(
                **event_encoding,
                opacity=alt.condition(event_hover, alt.value(0.26), alt.value(0.15)),
                stroke=alt.condition(event_hover, alt.value("#111827"), alt.value("transparent")),
            )
        )
        bands = bands.add_params(event_hover)
        if event_selection:
            bands = bands.add_params(event_selection)
        active_bands = [bands]
        if event_selection:
            active_bands.append(
                alt.Chart(event_data)
                .mark_rect(cursor="pointer", opacity=0.32, stroke="#111827", strokeWidth=1.5)
                .encode(**event_encoding)
                .transform_filter(event_selection)
            )
        chart_layers = active_bands + chart_layers

    chart = alt.layer(*chart_layers)
    if has_attention_legend:
        chart = chart.resolve_scale(color="independent")

    return (
        chart.properties(background="#f3f6f8", height=height)
        .configure_axis(
            domainColor="#cbd8e6",
            gridColor="#dfe8f1",
            labelColor="#344054",
            tickColor="#cbd8e6",
            titleColor="#0b2b63",
        )
        .configure_view(fill="#ffffff", stroke="#d9e2ec")
        .configure_legend(
            labelColor="#344054",
            orient="bottom",
            direction="horizontal",
            labelFontSize=13,
            titleColor="#344054",
        )
    )


def toggle_ontology_item(toggle_key: str) -> None:
    collapsed_items = set(st.session_state.get("collapsed_ontology", default_collapsed_ontology()))
    if toggle_key in collapsed_items:
        collapsed_items.remove(toggle_key)
    else:
        collapsed_items.add(toggle_key)
    st.session_state.collapsed_ontology = collapsed_items


def domain_display_name(topic_group: str) -> str:
    return dict(ONTOLOGY_DOMAINS).get(topic_group, topic_group.replace("_", " ").title())


def portland_ontology_rows(data: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows = data["overview_summary"][["topic_group", "label", "attention_share_pct"]].copy()
    rows["domain_label"] = rows["topic_group"].map(domain_display_name)
    domain_order = {topic_group: index for index, (topic_group, _) in enumerate(ONTOLOGY_DOMAINS)}
    rows["domain_order"] = rows["topic_group"].map(domain_order)
    return rows.sort_values(["domain_order", "attention_share_pct"], ascending=[True, False]).reset_index(drop=True)


def attention_column_for_group(topic_group: str) -> tuple[str, str]:
    return {
        "health": ("health_attention", "Health domain attention"),
        "environmental_health": ("environment_attention", "Environmental domain attention"),
        "social_structural": ("social_structural_attention", "Social/structural domain attention"),
    }[topic_group]


def summary_view_for_group(topic_group: str) -> str:
    return {
        "health": "overview_health",
        "environmental_health": "overview_environmental_health",
        "social_structural": "overview_social_structural",
    }[topic_group]


@st.cache_data
def load_chord_index() -> dict:
    return json.loads((PORTLAND_CHORD_DIR / "topic_chord_index.json").read_text(encoding="utf-8"))


@st.cache_data
def load_chord_topic(relative_file: str) -> dict:
    path = PORTLAND_CHORD_DIR / relative_file
    return json.loads(path.read_text(encoding="utf-8"))


def load_chord_template() -> str:
    return CHORD_TEMPLATE_PATH.read_text(encoding="utf-8")


def selected_chord_topic(selected_label: str, data: dict[str, pd.DataFrame]) -> dict | None:
    domain_selection = domain_from_selection(selected_label)
    if domain_selection is not None:
        rows = data["overview_summary"][data["overview_summary"]["topic_group"].eq(domain_selection)]
        if rows.empty:
            return None
        selected_label = str(rows.sort_values("attention_share_pct", ascending=False).iloc[0]["label"])
    index = load_chord_index()
    topic_lookup = {topic["topic"]: topic for topic in index["topics"]}
    return topic_lookup.get(selected_label)


def flatten_chord_links(chord: dict, intersection_limit: int) -> list[dict]:
    rows = []
    intersections = sorted(chord["intersections"], key=lambda item: float(item["overall_value"]), reverse=True)
    for intersection in intersections[:intersection_limit]:
        links = sorted(intersection.get("links", []), key=lambda item: float(item["value"]), reverse=True)
        for link in links:
            rows.append(
                {
                    "component_label": link["component_label"],
                    "intersecting_label": intersection["intersecting_label"],
                    "value": float(link["value"]),
                    "overall_value": float(intersection["overall_value"]),
                    "post_pct": float(intersection["percent_topic_posts_with_label"]),
                    "author_pct": float(intersection["percent_topic_authors_using_label_on_topic_post"]),
                    "post_contribution": float(link["post_percentage_point_contribution"]),
                    "author_contribution": float(link["author_percentage_point_contribution"]),
                }
            )
    return rows


def slug(value: str) -> str:
    return "".join(char.lower() if char.isalnum() else "-" for char in value).strip("-")


def arc_path(cx: float, cy: float, radius: float, start_deg: float, end_deg: float) -> str:
    start = polar_point(cx, cy, radius, start_deg)
    end = polar_point(cx, cy, radius, end_deg)
    large_arc = 1 if abs(end_deg - start_deg) > 180 else 0
    sweep = 1
    return f"M {start[0]:.2f} {start[1]:.2f} A {radius} {radius} 0 {large_arc} {sweep} {end[0]:.2f} {end[1]:.2f}"


def polar_point(cx: float, cy: float, radius: float, deg: float) -> tuple[float, float]:
    rad = math.radians(deg)
    return cx + radius * math.cos(rad), cy + radius * math.sin(rad)


def label_anchor(angle: float) -> str:
    return "end" if 90 < angle < 270 else "start"


def label_rotation(angle: float) -> float:
    rotation = angle
    if 90 < angle < 270:
        rotation += 180
    return rotation


def weighted_segments(values: list[tuple[str, float]], start: float, end: float, gap: float) -> dict[str, tuple[float, float, float]]:
    if not values:
        return {}
    total = sum(value for _, value in values) or 1
    usable = max(0, end - start - gap * (len(values) - 1))
    cursor = start
    segments = {}
    for label, value in values:
        span = max(2.0, usable * value / total)
        segment_end = min(end, cursor + span)
        segments[label] = (cursor, segment_end, (cursor + segment_end) / 2)
        cursor = segment_end + gap
    return segments


def chord_hover_css(component_labels: list[str], intersections: list[str]) -> str:
    rules = ['.chord-svg:has(.chord-node:hover) .chord-link { opacity: 0.045; }']
    for label in component_labels:
        label_slug = slug(label)
        rules.append(
            f'.chord-svg:has(.node-component-{label_slug}:hover) .link-component-{label_slug} '
            "{ opacity: 0.82; }"
        )
    for label in intersections:
        label_slug = slug(label)
        rules.append(
            f'.chord-svg:has(.node-intersection-{label_slug}:hover) .link-intersection-{label_slug} '
            "{ opacity: 0.82; }"
        )
    return "\n".join(rules)


def build_chord_svg(chord: dict, links: list[dict]) -> str:
    width, height = 940, 690
    cx, cy, radius = 470, 352, 252
    component_labels = [label for label in chord["component_labels"] if any(row["component_label"] == label for row in links)]
    intersections = []
    for row in links:
        if row["intersecting_label"] not in intersections:
            intersections.append(row["intersecting_label"])
    max_value = max((row["value"] for row in links), default=1)
    component_palette = [COLORS["orange"], COLORS["green"], COLORS["blue"], COLORS["purple"], COLORS["teal"], "#b7791f"]
    component_totals = {label: sum(row["value"] for row in links if row["component_label"] == label) for label in component_labels}
    intersection_totals = {label: max(row["overall_value"] for row in links if row["intersecting_label"] == label) for label in intersections}
    component_segments = weighted_segments([(label, component_totals[label]) for label in component_labels], -82, 82, 3.5)
    intersection_segments = weighted_segments([(label, intersection_totals[label]) for label in intersections], 108, 252, 1.2)
    component_angles = {label: segment[2] for label, segment in component_segments.items()}
    intersection_angles = {label: segment[2] for label, segment in intersection_segments.items()}
    component_colors = {
        label: component_palette[index % len(component_palette)]
        for index, label in enumerate(component_labels)
    }
    visible_intersection_labels = set(intersections[: min(10, len(intersections))])

    svg = [
        f'<svg class="chord-svg" viewBox="0 0 {width} {height}" role="img" aria-label="Chord diagram for {escape(chord["topic"])}">',
        "<defs>",
        '<filter id="softShadow" x="-20%" y="-20%" width="140%" height="140%"><feDropShadow dx="0" dy="1" stdDeviation="2" flood-color="#0f172a" flood-opacity="0.10"/></filter>',
        "</defs>",
        f'<circle cx="{cx}" cy="{cy}" r="{radius}" fill="#f8fafb" stroke="#d9e2ec" stroke-width="2"/>',
        f'<path d="{arc_path(cx, cy, radius + 28, -82, 82)}" fill="none" stroke="#c6d8f4" stroke-width="10" stroke-linecap="round" opacity="0.95"/>',
        f'<path d="{arc_path(cx, cy, radius + 22, 108, 252)}" fill="none" stroke="#e1e7ef" stroke-width="9" stroke-linecap="round" opacity="0.95"/>',
        f'<text x="{cx}" y="43" text-anchor="middle" class="chord-main-title">{escape(chord["topic"])} intersections by subgroup</text>',
        f'<text x="{polar_point(cx, cy, radius + 50, 2)[0]:.2f}" y="{polar_point(cx, cy, radius + 50, 2)[1]:.2f}" text-anchor="middle" class="chord-side-title">{escape(chord["topic"])}</text>',
        f'<text x="{polar_point(cx, cy, radius + 48, 182)[0]:.2f}" y="{polar_point(cx, cy, radius + 48, 182)[1]:.2f}" text-anchor="middle" class="chord-side-title muted">Intersecting labels</text>',
    ]

    for label, (start, end, angle) in component_segments.items():
        color = component_colors[label]
        svg.append(
            f'<g class="chord-node component-node node-component-{slug(label)}">'
            f'<path d="{arc_path(cx, cy, radius + 12, start, end)}" fill="none" stroke="{color}" stroke-width="16" stroke-linecap="round">'
            f'<title>{escape(label)} component total: {component_totals[label]:.2f}%</title></path></g>'
        )

    for label, (start, end, angle) in intersection_segments.items():
        svg.append(
            f'<g class="chord-node intersection-node node-intersection-{slug(label)}">'
            f'<path d="{arc_path(cx, cy, radius + 12, start, end)}" fill="none" stroke="#b9c4d2" stroke-width="11" stroke-linecap="round">'
            f'<title>{escape(label)} overall value: {intersection_totals[label]:.2f}%</title></path></g>'
        )

    for row in reversed(links):
        c_angle = component_angles[row["component_label"]]
        i_angle = intersection_angles[row["intersecting_label"]]
        c_outer = polar_point(cx, cy, radius - 10, c_angle)
        i_outer = polar_point(cx, cy, radius - 10, i_angle)
        c_inner = (cx - 86, cy)
        i_inner = (cx + 86, cy)
        stroke_width = 0.9 + 16 * (row["value"] / max_value)
        color = component_colors[row["component_label"]]
        comp_class = slug(row["component_label"])
        int_class = slug(row["intersecting_label"])
        tooltip = (
            f'{row["component_label"]} to {row["intersecting_label"]}\\n'
            f'Link value: {row["value"]:.2f}%\\n'
            f'Overall label value: {row["overall_value"]:.2f}%\\n'
            f'Posts with label: {row["post_pct"]:.2f}%\\n'
            f'Authors using label: {row["author_pct"]:.2f}%'
        )
        svg.append(
            f'<path class="chord-link link-component-{comp_class} link-intersection-{int_class}" d="M {c_outer[0]:.2f} {c_outer[1]:.2f} '
            f'C {c_inner[0]:.2f} {c_inner[1]:.2f}, {i_inner[0]:.2f} {i_inner[1]:.2f}, {i_outer[0]:.2f} {i_outer[1]:.2f}" '
            f'fill="none" stroke="{color}" stroke-width="{stroke_width:.2f}" stroke-linecap="round" opacity="0.30">'
            f"<title>{escape(tooltip)}</title></path>"
        )

    for label, angle in component_angles.items():
        lx, ly = polar_point(cx, cy, radius + 32, angle)
        rotation = label_rotation(angle)
        svg.append(
            f'<text x="{lx:.2f}" y="{ly:.2f}" text-anchor="{label_anchor(angle)}" dominant-baseline="middle" '
            f'transform="rotate({rotation:.2f} {lx:.2f} {ly:.2f})" class="chord-label chord-label-component">{escape(label)}</text>'
        )

    for label, angle in intersection_angles.items():
        if label not in visible_intersection_labels:
            continue
        lx, ly = polar_point(cx, cy, radius + 26, angle)
        rotation = label_rotation(angle)
        svg.append(
            f'<text x="{lx:.2f}" y="{ly:.2f}" text-anchor="{label_anchor(angle)}" dominant-baseline="middle" '
            f'transform="rotate({rotation:.2f} {lx:.2f} {ly:.2f})" class="chord-label">{escape(label)}</text>'
        )

    svg.append(f'<text x="{cx}" y="{cy - 6}" text-anchor="middle" class="chord-center-title">{len(links)}</text>')
    svg.append(f'<text x="{cx}" y="{cy + 22}" text-anchor="middle" class="chord-center-subtitle">component-label chords</text>')
    svg.append("</svg>")
    return "".join(svg)


def chord_panel(chord_meta: dict | None) -> tuple[dict | None, list[dict]]:
    if chord_meta is None:
        st.markdown(
            """
            <div>
                <div class="muted">Select a topic to load its chord diagram.</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        return None, []
    chord = load_chord_topic(chord_meta["file"])
    max_labels = min(50, len(chord["intersections"]))
    if max_labels:
        _, slider_col = st.columns([0.78, 0.22], gap="small", vertical_alignment="center")
        with slider_col:
            label_col, control_col = st.columns([0.30, 0.70], gap="small", vertical_alignment="center")
            with label_col:
                st.markdown('<div class="connections-slider-label">labels</div>', unsafe_allow_html=True)
            with control_col:
                label_limit = st.slider(
                    "labels",
                    min_value=1,
                    max_value=max_labels,
                    value=min(20, max_labels),
                    step=1,
                    key=f"connections_label_limit_{chord['category_id']}",
                    label_visibility="collapsed",
                )
    else:
        label_limit = 0
    chord_payload = {**chord, "_render_theme": "light_grey_v4", "_label_limit": label_limit}
    encoded = json.dumps(chord_payload, ensure_ascii=False, separators=(",", ":")).replace("</", "<\\/")
    html = load_chord_template().replace("__TOPIC_PAYLOAD__", encoded)
    links = flatten_chord_links(chord, label_limit)
    components.html(html, height=820, scrolling=False)
    return chord, links


def frozen_chord_context_panel(chord: dict | None, links: list[dict]) -> None:
    if chord is None:
        title = "Connection statistics"
        body = ""
        topic = "No topic selected"
        total_links = 0
        intersections = 0
        components_count = 0
    else:
        title = "Connection statistics"
        topic = chord["topic"]
        total_links = sum(len(item.get("links", [])) for item in chord["intersections"])
        intersections = len(chord["intersections"])
        components_count = len(chord["component_labels"])
        body = ""
    summary_html = f'<div class="summary-text">{escape(body)}</div>' if body else ""
    st.markdown(
        f"""
        <div class="panel">
            <div class="connection-panel-title">{title}</div>
            <div class="small-meta">{escape(topic)}</div>
            <div class="metric-grid">
                <div class="metric-card">
                    <div class="metric-label">Connections shown</div>
                    <div class="metric-value" style="color:#111827;">{len(links)}</div>
                    <div class="metric-note">top ranked</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">Available connections</div>
                    <div class="metric-value" style="color:#111827;">{total_links}</div>
                    <div class="metric-note">not post counts</div>
                </div>
            </div>
            <div style="height:.65rem"></div>
            <div class="metric-grid">
                <div class="metric-card">
                    <div class="metric-label">Topic labels</div>
                    <div class="metric-value" style="color:#111827;">{components_count}</div>
                    <div class="metric-note">topic labels</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">Labels</div>
                    <div class="metric-value" style="color:#111827;">{intersections}</div>
                    <div class="metric-note">labels</div>
                </div>
            </div>
            {summary_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def ontology_tree(active: bool, selected_label: str) -> None:
    data = load_portland_data(PORTLAND_DATA_VERSION)
    ontology_rows = portland_ontology_rows(data)
    collapsed_items = st.session_state.get("collapsed_ontology", default_collapsed_ontology())
    if active:
        st.markdown('<span class="connections-left-panel-marker"></span>', unsafe_allow_html=True)
        st.markdown('<div class="connection-panel-title">Topic selection</div>', unsafe_allow_html=True)
        st.markdown('<div class="muted" style="margin-top:.25rem">Click a category to load its chord diagram</div>', unsafe_allow_html=True)

        for topic_group, domain_label in ONTOLOGY_DOMAINS:
            domain_key = f"domain::{topic_group}"
            domain_collapsed = domain_key in collapsed_items
            domain_chev = "›" if domain_collapsed else "⌄"
            domain_selected = selected_label == domain_selection_value(topic_group)
            row = st.columns([0.08, 0.92], gap="small", vertical_alignment="center")
            with row[0]:
                if st.button(domain_chev, key=f"toggle_{domain_key}"):
                    toggle_ontology_item(domain_key)
                    st.rerun()
            with row[1]:
                domain_text = f"● {domain_label}" if domain_selected else domain_label
                if st.button(domain_text, key=f"title_{domain_key}"):
                    st.session_state.selected_label = domain_selection_value(topic_group)
                    if domain_collapsed:
                        toggle_ontology_item(domain_key)
                    st.query_params["page"] = "Ontology"
                    st.query_params["label"] = st.session_state.selected_label
                    st.rerun()
            if domain_collapsed:
                continue

            labels = ontology_rows[ontology_rows["topic_group"] == topic_group]["label"].tolist()
            for label in labels:
                label_text = f"■ {label}" if label == selected_label else f"□ {label}"
                row = st.columns([0.12, 0.88], gap="small", vertical_alignment="center")
                with row[1]:
                    if st.button(label_text, key=f"label_{topic_group}_{label}"):
                        st.session_state.selected_label = label
                        st.query_params["page"] = "Ontology"
                        st.query_params["label"] = label
                        st.rerun()
        return

    wrapper_class = "panel" if active else "panel disabled"
    html = [
        f'<div class="{wrapper_class}">',
        '<div class="connection-panel-title">Topic selection</div>',
        f'<div class="muted" style="margin-top:.55rem">{"Click a label to explore intersections" if active else "🔒 Available on Connections page"}</div>',
    ]

    for topic_group, domain_label in ONTOLOGY_DOMAINS:
        domain_key = f"domain::{topic_group}"
        domain_collapsed = domain_key in collapsed_items
        domain_chev = "›" if domain_collapsed else "⌄"
        domain_toggle = f'<span class="chev">{domain_chev}</span>'
        selected = " selected" if selected_label == domain_selection_value(topic_group) else ""
        html.append(f'<div class="tree-row dim{selected}">{domain_toggle}<span class="dot"></span>{escape(domain_label)}</div>')
        if domain_collapsed:
            continue
        labels = ontology_rows[ontology_rows["topic_group"] == topic_group]["label"].tolist()
        for label in labels:
            selected = " selected" if label == selected_label else ""
            html.append(f'<div class="tree-row label{selected}"><span class="box"></span>{escape(label)}</div>')
    html.append("</div>")
    st.markdown("".join(html), unsafe_allow_html=True)


def intersection_panel(seed_label: str) -> None:
    data = load_portland_data(PORTLAND_DATA_VERSION)["overview_summary"]
    groups = [
        ("Health", "health", COLORS["teal"], ""),
        ("Environmental", "environmental_health", COLORS["blue"], "blue"),
        ("Social", "social_structural", COLORS["purple"], "purple"),
    ]
    html = ['<div class="panel"><h3 style="margin-top:0">Intersecting labels</h3><div class="intersection-grid">']
    for display, topic_group, color, class_name in groups:
        rows = data[data["topic_group"] == topic_group].copy()
        rows = rows[~rows["label"].str.casefold().eq(seed_label.casefold())]
        rows = rows.sort_values("attention_share_pct", ascending=False).head(3)
        max_share = rows["attention_share_pct"].max() if not rows.empty else 1
        title_class = f"mini-title {class_name}".strip()
        html.append(f'<div class="mini-card"><div class="{title_class}">{display}</div>')
        for _, row in rows.iterrows():
            intersection_pct = int(round(row["attention_share_pct"]))
            width = max(8, min(100, int(row["attention_share_pct"] / max_share * 100)))
            html.append(
                '<div class="bar-row">'
                f'<div class="bar-row-top"><span>{escape(row["label"])}</span><span>{intersection_pct}%</span></div>'
                f'<div class="bar-track"><div class="bar-fill" style="width:{width}%;background:{color};"></div></div>'
                "</div>"
            )
        html.append("</div>")
    html.append("</div></div>")
    st.markdown("".join(html), unsafe_allow_html=True)


def overview_domain_label(topic_group: str) -> str:
    return {
        "health": "Health",
        "environmental_health": "Environmental",
        "social_structural": "Social",
    }.get(topic_group, domain_display_name(topic_group))


def top_score_rows(
    data: dict[str, pd.DataFrame],
    selected_label: str,
    score_type: str,
) -> pd.DataFrame:
    rows = data["topic_label_scores"]
    rows = rows[(rows["topic"] == selected_label) & (rows["score_type"] == score_type)].copy()
    return rows.sort_values(["value", "label"], ascending=[False, True]).head(3)


def top_labels_panel(selected_label: str, data: dict[str, pd.DataFrame]) -> None:
    groups = [
        ("Children", "child_population", "black", COLORS["slate"]),
        ("Current topic", "topic_component", "black", COLORS["slate"]),
        ("Place", "setting", "black", COLORS["slate"]),
    ]

    html = ['<div class="panel"><h3 style="margin-top:0">Top 3 labels</h3><div class="top-label-grid">']
    for title, score_type, class_name, color in groups:
        score_rows = top_score_rows(data, selected_label, score_type)
        max_score = score_rows["value"].max() if not score_rows.empty else 1
        title_class = f"mini-title {class_name}".strip()
        html.append(f'<div class="mini-card"><div class="{title_class}">{title}</div>')
        for _, row in score_rows.iterrows():
            label = str(row["label"])
            pct = int(round(row["value"]))
            width = max(8, min(100, int(row["value"] / max_score * 100)))
            html.append(
                '<div class="bar-row">'
                f'<div class="bar-row-top"><span>{escape(label)}</span><span>{pct}%</span></div>'
                f'<div class="bar-track"><div class="bar-fill" style="width:{width}%;background:{color};"></div></div>'
                "</div>"
            )
        for _ in range(max(0, 3 - len(score_rows))):
            html.append('<div class="bar-row">&nbsp;<div class="bar-track"></div></div>')
        html.append("</div>")
    html.append("</div></div>")
    st.markdown("".join(html), unsafe_allow_html=True)


def metric_cards(selected_label: str) -> None:
    summary = load_data(DATA_VERSION)["label_summary"]
    row = summary[summary["label"] == selected_label].iloc[0]
    st.markdown(
        f"""
        <div class="metric-grid">
            <div class="metric-card">
                <div class="metric-label">Average rank</div>
                <div class="metric-value">{row['average_rank_display']}</div>
                <div class="metric-note">across full range</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Max rank</div>
                <div class="metric-value">{row['max_rank_display']}</div>
                <div class="metric-note">peak month</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def portland_metric_cards(selected_label: str, data: dict[str, pd.DataFrame]) -> None:
    summary = data["label_summary"]
    row = summary[summary["label"] == selected_label]
    if row.empty:
        return
    row = row.iloc[0]
    topic_group = str(row["topic_group"])
    metric_color = overview_color_map(topic_group, data=data).get(selected_label, label_color(selected_label))
    st.markdown(
        f"""
        <div class="metric-grid">
            <div class="metric-card">
                <div class="metric-label">Average rank</div>
                <div class="metric-value" style="color:{metric_color};">{row['average_rank_display']}</div>
                <div class="metric-note">across full range</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Max rank</div>
                <div class="metric-value" style="color:{metric_color};">{row['max_rank_display']}</div>
                <div class="metric-note">peak month</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def overview_context_panel(
    selected_labels: list[str],
    group: str,
    summary_view: str,
    selected_event: str | None,
    data: dict[str, pd.DataFrame],
) -> None:
    selected_label = selected_labels[0] if selected_labels else default_portland_label(data)
    if selected_event:
        rank_summary_panel(
            "",
            group,
            summary_view,
            labels=selected_labels,
            min_height=OVERVIEW_SUMMARY_MIN_HEIGHT,
            selected_event=selected_event,
            data=data,
        )
        return
    top_labels_panel(selected_label, data)
    portland_metric_cards(selected_label, data)


def summary_box(view: str, label: str | None = None) -> None:
    data = load_data(DATA_VERSION)["summaries"]
    if label:
        row = data[(data["view"] == view) & (data["label"] == label)]
    else:
        row = data[data["view"] == view]
    text = row.iloc[0]["summary"]
    st.markdown(
        f"""
        <div class="panel">
            <h3 style="margin-top:0">Generated Summary</h3>
            <div class="summary-text">{text}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def selected_event_id(chart_state, selection_name: str) -> str | None:
    if not chart_state:
        return None
    state = dict(chart_state)
    selection = state.get("selection", {})
    selected = selection.get(selection_name)
    if not selected:
        return None
    if isinstance(selected, dict):
        event_ids = selected.get("event_id")
        if isinstance(event_ids, list) and event_ids:
            return str(event_ids[0])
        if isinstance(event_ids, str):
            return event_ids
    if isinstance(selected, list) and selected:
        first = selected[0]
        if isinstance(first, dict) and first.get("event_id"):
            return str(first["event_id"])
    return None


def has_events_for_labels(events: pd.DataFrame, labels: list[str]) -> bool:
    return not events[events["label"].isin(labels)].empty


def event_labels_for_group(events: pd.DataFrame, group: str) -> list[str]:
    if "topic_group" not in events.columns:
        return []
    return events.loc[events["topic_group"].eq(group), "label"].drop_duplicates().tolist()


def selectable_altair_chart(
    chart: alt.Chart,
    key: str,
    selection_name: str,
    has_selectable_events: bool,
):
    if has_selectable_events:
        return st.altair_chart(
            chart,
            theme=None,
            use_container_width=True,
            key=key,
            on_select="rerun",
            selection_mode=selection_name,
        )
    st.altair_chart(
        chart,
        theme=None,
        use_container_width=True,
        key=key,
    )
    return None


def source_links_html(raw_urls: str, raw_titles: str | None = None) -> str:
    urls = [
        url.strip()
        for url in str(raw_urls).replace("|", ";").replace(",", ";").split(";")
        if url.strip()
    ]
    if not urls:
        return ""
    titles = [
        title.strip()
        for title in str(raw_titles or "").replace("|", ";").replace(",", ";").split(";")
        if title.strip()
    ]
    items = "".join(
        f'<li><a href="{escape(url)}" target="_blank" rel="noopener noreferrer">{escape(titles[index] if index < len(titles) else default_link_title(url))}</a></li>'
        for index, url in enumerate(urls)
    )
    return f'<ul class="source-links">{items}</ul>'


def rank_summary_panel(
    title: str,
    group: str,
    summary_view: str,
    labels: list[str] | None = None,
    min_height: int | None = None,
    selected_event: str | None = None,
    data: dict[str, pd.DataFrame] | None = None,
) -> None:
    data = data or load_data(DATA_VERSION)
    event = pd.DataFrame()
    if selected_event:
        event = data["events"][data["events"]["event_id"] == selected_event]
        if labels and not event.empty:
            matching_topic_event = event[event["label"].isin(labels)]
            if not matching_topic_event.empty:
                event = matching_topic_event
    if not event.empty:
        event_row = event.iloc[0]
        title = escape(event_row["event_name"])
        title_color = overview_color_map(group, data=data).get(event_row["label"], label_color(event_row["label"]))
        title_style = f"margin-top:0;color:{title_color};"
        text = (
            f'<p>{escape(event_row["event_summary"])}</p>'
            f'{source_links_html(event_row["event_url"], event_row.get("event_url_title"))}'
        )
        summary_prefix = ""
    else:
        title_style = "margin-top:0"
        text = data["summaries"][data["summaries"]["view"] == summary_view].iloc[0]["summary"]
        summary_prefix = "✧ "
    min_height_style = f"min-height:{min_height}px;" if min_height else ""
    st.markdown(
        f"""
        <div class="panel" style="{min_height_style}">
            <h3 style="{title_style}">{title}</h3>
            <div class="summary-text">{summary_prefix}{text}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def parse_top_percent(display_value: str) -> int:
    digits = "".join(char for char in display_value if char.isdigit())
    return int(digits) if digits else 100


def overview_profile_panel_html(title: str, group: str, note: str, panel_class: str) -> str:
    rows = load_data(DATA_VERSION)["overview_summary"]
    rows = rows[rows["topic_group"] == group].head(5).copy()
    if rows.empty:
        return ""

    rows["average_top_pct"] = rows["average_rank_display"].map(parse_top_percent)
    rows["bar_width"] = (100 - rows["average_top_pct"]).clip(lower=4, upper=100)
    max_width = rows["bar_width"].max()
    rows["bar_width"] = (rows["bar_width"] / max_width * 100).round().astype(int)

    html_rows = "".join(
        f"""
<div class="overview-profile-row">
<div class="overview-profile-row-top">
<span class="overview-profile-label">{escape(row['label'])}</span>
<span class="overview-profile-value">{escape(row['average_rank_display'])}</span>
</div>
<div class="overview-profile-track">
<div class="overview-profile-fill" style="width:{row['bar_width']}%;background:{label_color(row['label'])};"></div>
</div>
</div>
"""
        for _, row in rows.iterrows()
    )
    note_html = f'<div class="overview-profile-meta">{note}</div>' if note else ""
    return f"""
<div class="overview-profile-panel {panel_class}">
<h3>{title}</h3>
{note_html}
<div class="overview-profile-bars">{html_rows}</div>
</div>
"""


def overview_profile_stack() -> None:
    health_panel = overview_profile_panel_html(
        "Health topics",
        "health",
        "",
        "top",
    )
    environment_panel = overview_profile_panel_html(
        "Environmental topics",
        "environmental_health",
        "",
        "bottom",
    )
    st.markdown(
        f"""
        <div class="overview-profile-stack">
            {health_panel}
            {environment_panel}
        </div>
        """,
        unsafe_allow_html=True,
    )


def overview_topic_rows(
    group: str,
    limit: int | None = None,
    data: dict[str, pd.DataFrame] | None = None,
) -> pd.DataFrame:
    data = data or load_data(DATA_VERSION)
    rows = data["overview_summary"]
    rows = rows[rows["topic_group"] == group].copy()
    if limit is not None:
        rows = rows.head(limit)
    if "attention_share_pct" in rows.columns:
        rows["bar_metric_pct"] = rows["attention_share_pct"].fillna(0)
        rows["bar_metric_display"] = rows["attention_share_display"].fillna("0.0%")
        rows["bar_width"] = rows["bar_metric_pct"].clip(lower=0, upper=100)
    else:
        rows["average_top_pct"] = rows["average_rank_display"].map(parse_top_percent)
        rows["bar_width"] = (100 - rows["average_top_pct"]).clip(lower=4, upper=100)
        rows["bar_metric_display"] = rows["average_rank_display"]
    max_width = rows["bar_width"].max()
    if max_width > 0:
        rows["bar_width"] = (rows["bar_width"] / max_width * 100).round().astype(int)
    else:
        rows["bar_width"] = 0
    return rows


def overview_selection_key(group: str) -> str:
    return f"overview_selected_{group}"


def ensure_overview_selection(group: str, data: dict[str, pd.DataFrame] | None = None) -> list[str]:
    key = overview_selection_key(group)
    available = overview_topic_rows(group, data=data)["label"].tolist()
    if key not in st.session_state:
        st.session_state[key] = available[0] if available else None
    stored = st.session_state[key]
    if isinstance(stored, list):
        stored = stored[0] if stored else None
    selected = stored if stored in available else (available[0] if available else None)
    st.session_state[key] = selected
    return [selected] if selected else []


def toggle_overview_topic(group: str, label: str) -> None:
    key = overview_selection_key(group)
    st.session_state[key] = label


def overview_profile_panel(title: str, group: str, data: dict[str, pd.DataFrame] | None = None) -> list[str]:
    rows = overview_topic_rows(group, data=data)
    selected = ensure_overview_selection(group, data=data)
    colors = overview_color_map(group, data=data)

    st.markdown(
        f'<h3 class="overview-profile-title"><span class="overview-profile-panel-marker"></span>{title}</h3>',
        unsafe_allow_html=True,
    )
    with st.container(height=OVERVIEW_BAR_LIST_HEIGHT, border=False):
        for _, row in rows.iterrows():
            label = row["label"]
            row_class = "" if label in selected else " overview-profile-row-muted"
            top_cols = st.columns([0.72, 0.28], gap="small", vertical_alignment="bottom")
            with top_cols[0]:
                marker = "●" if label in selected else "○"
                if st.button(
                    f"{marker} {label}",
                    key=f"overview_toggle_{group}_{label}",
                ):
                    toggle_overview_topic(group, label)
                    st.rerun()
            with top_cols[1]:
                st.markdown(
                    f'<div class="overview-profile-value">{escape(row["bar_metric_display"])}</div>',
                    unsafe_allow_html=True,
                )
            st.markdown(
                f"""
<div class="overview-profile-track{row_class}">
<div class="overview-profile-fill" style="width:{row['bar_width']}%;background:{colors.get(label, label_color(label))};"></div>
</div>
""",
                unsafe_allow_html=True,
            )
    return selected


def header() -> str:
    page = st.session_state.get("current_page", "Overview")
    display_page = "Connections" if page == "Ontology" else page
    if st.session_state.get("page_switch") == "Ontology":
        st.session_state.page_switch = "Connections"
    if st.session_state.get("page_switch") not in {"Overview", "Connections"}:
        st.session_state.page_switch = display_page

    left, right = st.columns([0.58, 0.42], vertical_alignment="center")
    with left:
        st.title("Children's Environmental Health Topics")
    with right:
        c1, c2 = st.columns([0.42, 0.58], vertical_alignment="top")
        with c1:
            st.selectbox("City / region", ["Portland CBSA"], index=0)
        with c2:
            selected_display_page = st.segmented_control(
                "View",
                ["Overview", "Connections"],
                key="page_switch",
                selection_mode="single",
                required=True,
                width="stretch",
            )
            selected_page = "Ontology" if selected_display_page == "Connections" else selected_display_page
            if selected_page and selected_page != page:
                st.session_state.current_page = selected_page
                st.query_params["page"] = selected_page
                st.query_params["label"] = st.session_state.get("selected_label", "wildfire smoke")
                st.rerun()
            page = selected_page or page
    return page


def overview_page() -> None:
    data = load_portland_data(PORTLAND_DATA_VERSION)
    health_colors = overview_color_map("health", data=data)
    environmental_colors = overview_color_map("environmental_health", data=data)
    social_colors = overview_color_map("social_structural", data=data)
    profile_col, chart_col, summary_col = st.columns([0.22, 0.48, 0.30], gap="medium", vertical_alignment="top")
    with profile_col:
        health_labels = overview_profile_panel("Health topics", "health", data=data)
    with chart_col:
        with st.container(border=True):
            title_col, toggle_col = st.columns([0.68, 0.32], gap="small", vertical_alignment="center")
            with title_col:
                st.markdown(
                    '<div class="chart-title">Top children\'s health topics over time</div>',
                    unsafe_allow_html=True,
                )
            with toggle_col:
                health_local_only = local_events_only_control("health_local_events_only")
            health_events = filter_local_events(data["events"], health_local_only)
            health_has_events = has_events_for_labels(health_events, health_labels)
            health_chart_state = selectable_altair_chart(
                line_chart(
                    data["timeseries"],
                    "",
                    health_labels,
                    events=health_events,
                    event_labels=health_labels,
                    height=OVERVIEW_CHART_HEIGHT,
                    attention_column="health_attention",
                    attention_label="Health attention",
                    value_column="attention_weighted_topic_value",
                    y_axis_title="Attention-weighted topic value",
                    color_map=health_colors,
                    event_selection_name="health_event_select" if health_has_events else None,
                ),
                key="health_time_series",
                selection_name="health_event_select",
                has_selectable_events=health_has_events,
            )
    with summary_col:
        overview_context_panel(
            health_labels,
            "health",
            "overview_health",
            selected_event_id(health_chart_state, "health_event_select"),
            data,
        )

    profile_col, chart_col, summary_col = st.columns([0.22, 0.48, 0.30], gap="medium", vertical_alignment="top")
    with profile_col:
        environmental_labels = overview_profile_panel("Environmental topics", "environmental_health", data=data)
    with chart_col:
        with st.container(border=True):
            title_col, toggle_col = st.columns([0.68, 0.32], gap="small", vertical_alignment="center")
            with title_col:
                st.markdown(
                    '<div class="chart-title">Top children\'s environmental-health topics over time</div>',
                    unsafe_allow_html=True,
                )
            with toggle_col:
                environmental_local_only = local_events_only_control("environmental_local_events_only")
            environmental_events = filter_local_events(data["events"], environmental_local_only)
            environmental_has_events = has_events_for_labels(environmental_events, environmental_labels)
            environmental_chart_state = selectable_altair_chart(
                line_chart(
                    data["timeseries"],
                    "",
                    environmental_labels,
                    events=environmental_events,
                    event_labels=environmental_labels,
                    height=OVERVIEW_CHART_HEIGHT,
                    attention_column="environment_attention",
                    attention_label="Environment attention",
                    value_column="attention_weighted_topic_value",
                    y_axis_title="Attention-weighted topic value",
                    color_map=environmental_colors,
                    event_selection_name="environment_event_select" if environmental_has_events else None,
                ),
                key="environment_time_series",
                selection_name="environment_event_select",
                has_selectable_events=environmental_has_events,
            )
    with summary_col:
        overview_context_panel(
            environmental_labels,
            "environmental_health",
            "overview_environmental_health",
            selected_event_id(environmental_chart_state, "environment_event_select"),
            data,
        )

    profile_col, chart_col, summary_col = st.columns([0.22, 0.48, 0.30], gap="medium", vertical_alignment="top")
    with profile_col:
        social_labels = overview_profile_panel("Social environment topics", "social_structural", data=data)
    with chart_col:
        with st.container(border=True):
            title_col, toggle_col = st.columns([0.68, 0.32], gap="small", vertical_alignment="center")
            with title_col:
                st.markdown(
                    '<div class="chart-title">Top children\'s social-environment topics over time</div>',
                    unsafe_allow_html=True,
                )
            with toggle_col:
                social_local_only = local_events_only_control("social_local_events_only")
            social_events = filter_local_events(data["events"], social_local_only)
            social_has_events = has_events_for_labels(social_events, social_labels)
            social_chart_state = selectable_altair_chart(
                line_chart(
                    data["timeseries"],
                    "",
                    social_labels,
                    events=social_events,
                    event_labels=social_labels,
                    height=OVERVIEW_CHART_HEIGHT,
                    attention_column="social_structural_attention",
                    attention_label="Social environment attention",
                    value_column="attention_weighted_topic_value",
                    y_axis_title="Attention-weighted topic value",
                    color_map=social_colors,
                    event_selection_name="social_event_select" if social_has_events else None,
                ),
                key="social_time_series",
                selection_name="social_event_select",
                has_selectable_events=social_has_events,
            )
    with summary_col:
        overview_context_panel(
            social_labels,
            "social_structural",
            "overview_social_structural",
            selected_event_id(social_chart_state, "social_event_select"),
            data,
        )


def ontology_page() -> None:
    data = load_portland_data(PORTLAND_DATA_VERSION)
    selected_label = st.session_state.selected_label
    available = set(data["overview_summary"]["label"])
    domain_selection = domain_from_selection(selected_label)
    if selected_label not in available and domain_selection is None:
        selected_label = default_portland_label(data)
        st.session_state.selected_label = selected_label
        domain_selection = None
    chord_meta = selected_chord_topic(selected_label, data)
    left, center, right = st.columns([0.19, 0.59, 0.22], gap="medium")
    with left:
        ontology_tree(active=True, selected_label=selected_label)
    with center:
        st.markdown('<span class="connections-center-panel-marker"></span>', unsafe_allow_html=True)
        st.markdown('<div class="connection-panel-title">Topic label connections</div>', unsafe_allow_html=True)
        chord, links = chord_panel(chord_meta)
    with right:
        frozen_chord_context_panel(chord, links)


def footer() -> None:
    st.markdown(
        """
        <div class="footer">
            <div class="shield">✓</div>
            <div><strong>AI and event-match disclaimer:</strong> Summaries were generated using AI and may not be accurate.
            Candidate event matches are provisional and should be reviewed against public incident records. Results describe
            relative topic prominence within a screened child-relevant corpus, not total public discourse.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def main() -> None:
    inject_css()
    query_page = st.query_params.get("page")
    if query_page == "Connections":
        query_page = "Ontology"
    if query_page not in {"Overview", "Ontology"}:
        query_page = "Overview"
    if "current_page" not in st.session_state:
        st.session_state.current_page = query_page
    if "page_switch" not in st.session_state:
        st.session_state.page_switch = "Connections" if st.session_state.current_page == "Ontology" else st.session_state.current_page
    portland_data = load_portland_data(PORTLAND_DATA_VERSION)
    if "selected_label" not in st.session_state:
        st.session_state.selected_label = default_portland_label(portland_data)
    if st.session_state.get("ontology_collapse_state_version") != ONTOLOGY_COLLAPSE_STATE_VERSION:
        st.session_state.collapsed_ontology = default_collapsed_ontology()
        st.session_state.ontology_collapse_state_version = ONTOLOGY_COLLAPSE_STATE_VERSION
    if "collapsed_ontology" not in st.session_state:
        st.session_state.collapsed_ontology = default_collapsed_ontology()
    query_label = st.query_params.get("label")
    if query_label:
        available = set(portland_data["overview_summary"]["label"])
        if query_label in available or domain_from_selection(query_label) is not None:
            st.session_state.selected_label = query_label
    page = header()
    if page == "Overview":
        overview_page()
    else:
        ontology_page()
    footer()


if __name__ == "__main__":
    main()

