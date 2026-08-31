from __future__ import annotations

from html import escape
from pathlib import Path
from urllib.parse import urlparse

import altair as alt
import pandas as pd
import streamlit as st


APP_DIR = Path(__file__).resolve().parent
DATA_DIR = APP_DIR / "data"
PORTLAND_DATA_DIR = DATA_DIR / "portland"
DATA_VERSION = "synthetic_bar_attention_shares_v1"
PORTLAND_DATA_VERSION = "portland_dashboard_packet_v007_prepared_v1"

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
ONTOLOGY_DIMENSIONS = ["exposure", "health condition", "setting", "activity or intervention"]
ONTOLOGY_COLLAPSE_STATE_VERSION = "all_collapsed_v1"


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
        "summaries": pd.read_parquet(PORTLAND_DATA_DIR / "generated_summaries.parquet"),
    }


def default_collapsed_ontology() -> set[str]:
    ontology = load_data(DATA_VERSION)["ontology"]
    collapsed = {f"dimension::{dimension}" for dimension in ONTOLOGY_DIMENSIONS}
    collapsed.update(
        f"parent::{row.dimension}::{row.parent_label}"
        for row in ontology[["dimension", "parent_label"]].drop_duplicates().itertuples(index=False)
    )
    return collapsed


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
        div[data-testid="stVerticalBlock"] { gap: 0.7rem; }
        .panel {
            background: #ffffff;
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
        .panel.disabled .disabled-link {
            color: #98a2b3 !important;
            border-color: #d6dee8;
            background: #eef2f6;
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
        .clear-link {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            border: 1px solid #cbd8e6;
            border-radius: 8px;
            padding: 0.38rem 0.62rem;
            color: #0d4f73 !important;
            text-decoration: none !important;
            font-size: 0.78rem;
            font-weight: 700;
            white-space: nowrap;
            background: #ffffff;
        }
        .disabled-link {
            color: #8a96a8 !important;
            background: #f5f7fa;
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
            background: #ffffff;
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
            color: #33415c;
            font-size: 0.78rem;
            margin-top: 0.05rem;
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
        .overview-profile-panel-marker {
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
    if plot.empty:
        fallback = df[df["label"] == "wildfire smoke"].copy()
        fallback["label"] = labels[0]
        plot = fallback
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

    chart_layers = [lines]
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


def ontology_tree(active: bool, selected_label: str) -> None:
    data = load_data(DATA_VERSION)["ontology"]
    collapsed_items = st.session_state.get("collapsed_ontology", default_collapsed_ontology())
    if active:
        with st.container(border=True):
            header_cols = st.columns([0.58, 0.42], gap="small", vertical_alignment="center")
            with header_cols[0]:
                st.markdown('<h3 style="margin:0">Ontology</h3>', unsafe_allow_html=True)
            with header_cols[1]:
                if st.button("↻ Clear", key="clear_ontology", use_container_width=True):
                    st.session_state.selected_label = "wildfire smoke"
                    st.session_state.collapsed_ontology = default_collapsed_ontology()
                    st.query_params["page"] = "Ontology"
                    st.query_params["label"] = "wildfire smoke"
                    st.rerun()
            st.markdown('<div class="muted" style="margin-top:.25rem">Click a label to explore intersections</div>', unsafe_allow_html=True)

            for dimension in ONTOLOGY_DIMENSIONS:
                dim_label = dimension.title() if dimension != "activity or intervention" else "Activity or intervention"
                dim_key = f"dimension::{dimension}"
                dim_collapsed = dim_key in collapsed_items
                dim_chev = "›" if dim_collapsed else "⌄"
                row = st.columns([0.08, 0.92], gap="small", vertical_alignment="center")
                with row[0]:
                    if st.button(dim_chev, key=f"toggle_{dim_key}"):
                        toggle_ontology_item(dim_key)
                        st.rerun()
                with row[1]:
                    if st.button(dim_label, key=f"title_{dim_key}"):
                        toggle_ontology_item(dim_key)
                        st.rerun()
                if dim_collapsed:
                    continue

                parents = data[data["dimension"] == dimension]["parent_label"].drop_duplicates().tolist()
                for parent in parents:
                    parent_key = f"parent::{dimension}::{parent}"
                    collapsed = parent_key in collapsed_items
                    chev = "›" if collapsed else "⌄"
                    row = st.columns([0.09, 0.08, 0.83], gap="small", vertical_alignment="center")
                    with row[1]:
                        if st.button(chev, key=f"toggle_{parent_key}"):
                            toggle_ontology_item(parent_key)
                            st.rerun()
                    with row[2]:
                        if st.button(parent, key=f"title_{parent_key}"):
                            toggle_ontology_item(parent_key)
                            st.rerun()
                    if collapsed:
                        continue

                    labels = data[(data["dimension"] == dimension) & (data["parent_label"] == parent)]["canonical_label"].tolist()
                    for label in labels:
                        label_text = f"■ {label}" if label == selected_label else f"□ {label}"
                        row = st.columns([0.17, 0.83], gap="small", vertical_alignment="center")
                        with row[1]:
                            if st.button(label_text, key=f"label_{dimension}_{parent}_{label}"):
                                st.session_state.selected_label = label
                                st.query_params["page"] = "Ontology"
                                st.query_params["label"] = label
                                st.rerun()
        return

    wrapper_class = "panel" if active else "panel disabled"
    clear_button = (
        '<span class="clear-link disabled-link">↻ Clear selection</span>'
        if active
        else '<span class="clear-link disabled-link">↻ Clear selection</span>'
    )
    html = [
        f'<div class="{wrapper_class}">',
        '<div style="display:flex;align-items:center;justify-content:space-between;gap:.75rem;">'
        '<h3 style="margin:0">Ontology</h3>'
        f"{clear_button}"
        "</div>",
        f'<div class="muted" style="margin-top:.55rem">{"Click a label to explore intersections" if active else "🔒 Available on Ontology page"}</div>',
    ]

    for dimension in ONTOLOGY_DIMENSIONS:
        dim_label = dimension.title() if dimension != "activity or intervention" else "Activity or intervention"
        dim_key = f"dimension::{dimension}"
        dim_collapsed = dim_key in collapsed_items
        dim_chev = "›" if dim_collapsed else "⌄"
        dim_toggle = f'<span class="chev">{dim_chev}</span>'
        html.append(f'<div class="tree-row dim">{dim_toggle}<span class="dot"></span>{escape(dim_label)}</div>')
        if dim_collapsed:
            continue
        parents = data[data["dimension"] == dimension]["parent_label"].drop_duplicates().tolist()
        for parent in parents:
            parent_key = f"parent::{dimension}::{parent}"
            collapsed = parent_key in collapsed_items
            chev = "›" if collapsed else "⌄"
            parent_toggle = f'<span class="chev">{chev}</span>'
            html.append(f'<div class="tree-row parent">{parent_toggle}<span class="dot"></span>{escape(parent)}</div>')
            if collapsed:
                continue
            labels = data[(data["dimension"] == dimension) & (data["parent_label"] == parent)]["canonical_label"].tolist()
            for label in labels:
                selected = " selected" if label == selected_label else ""
                html.append(f'<div class="tree-row label{selected}"><span class="box"></span>{escape(label)}</div>')
    html.append("</div>")
    st.markdown("".join(html), unsafe_allow_html=True)


def intersection_panel(seed_label: str) -> None:
    data = load_data(DATA_VERSION)["intersections"]
    subset = data[data["seed_label"] == seed_label]
    groups = [
        ("Health condition", "health condition", COLORS["teal"], ""),
        ("Setting", "setting", COLORS["blue"], "blue"),
        ("Activity", "activity", COLORS["purple"], "purple"),
    ]
    html = ['<div class="panel"><h3 style="margin-top:0">Intersecting labels</h3><div class="intersection-grid">']
    for display, dim, color, class_name in groups:
        rows = subset[subset["dimension"] == dim]
        title_class = f"mini-title {class_name}".strip()
        html.append(f'<div class="mini-card"><div class="{title_class}">{display}</div>')
        for _, row in rows.iterrows():
            width = max(8, min(100, int(row["intersection_pct"])))
            html.append(
                '<div class="bar-row">'
                f'<div class="bar-row-top"><span>{row["label"]}</span><span>{int(row["intersection_pct"])}%</span></div>'
                f'<div class="bar-track"><div class="bar-fill" style="width:{width}%;background:{color};"></div></div>'
                "</div>"
            )
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

    left, right = st.columns([0.58, 0.42], vertical_alignment="center")
    with left:
        st.title("Children's Environmental Health Topics")
    with right:
        c1, c2 = st.columns([0.42, 0.58], vertical_alignment="top")
        with c1:
            st.selectbox("City / region", ["Portland CBSA"], index=0)
        with c2:
            selected_page = st.segmented_control(
                "View",
                ["Overview", "Ontology"],
                key="page_switch",
                selection_mode="single",
                required=True,
                width="stretch",
            )
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
        rank_summary_panel(
            "Health topic summary",
            "health",
            "overview_health",
            labels=health_labels,
            min_height=OVERVIEW_SUMMARY_MIN_HEIGHT,
            selected_event=selected_event_id(health_chart_state, "health_event_select"),
            data=data,
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
        rank_summary_panel(
            "Environmental-health summary",
            "environmental_health",
            "overview_environmental_health",
            labels=environmental_labels,
            min_height=OVERVIEW_SUMMARY_MIN_HEIGHT,
            selected_event=selected_event_id(environmental_chart_state, "environment_event_select"),
            data=data,
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
        rank_summary_panel(
            "Social-environment summary",
            "social_structural",
            "overview_social_structural",
            labels=social_labels,
            min_height=OVERVIEW_SUMMARY_MIN_HEIGHT,
            selected_event=selected_event_id(social_chart_state, "social_event_select"),
            data=data,
        )


def ontology_page() -> None:
    data = load_data(DATA_VERSION)
    selected_label = st.session_state.selected_label
    left, center, right = st.columns([0.23, 0.43, 0.34], gap="medium")
    with left:
        ontology_tree(active=True, selected_label=selected_label)
    with center:
        with st.container(border=True):
            st.markdown(
                f'<div class="chart-title">{selected_label.capitalize()} rank within child environmental-health topics</div>'
                '<div class="chart-meta">Fixed view: monthly rank percentile &nbsp;|&nbsp; full available date range</div>',
                unsafe_allow_html=True,
            )
            st.altair_chart(
                line_chart(
                    data["timeseries"],
                    "",
                    [selected_label],
                    events=data["events"],
                    height=ONTOLOGY_CHART_HEIGHT,
                ),
                theme=None,
                use_container_width=True,
            )
    with right:
        intersection_panel("wildfire smoke")
        metric_cards("wildfire smoke")
        summary_box("ontology", "wildfire smoke")


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
    if query_page not in {"Overview", "Ontology"}:
        query_page = "Overview"
    if "current_page" not in st.session_state:
        st.session_state.current_page = query_page
    if "page_switch" not in st.session_state:
        st.session_state.page_switch = st.session_state.current_page
    if "selected_label" not in st.session_state:
        st.session_state.selected_label = "wildfire smoke"
    if st.session_state.get("ontology_collapse_state_version") != ONTOLOGY_COLLAPSE_STATE_VERSION:
        st.session_state.collapsed_ontology = default_collapsed_ontology()
        st.session_state.ontology_collapse_state_version = ONTOLOGY_COLLAPSE_STATE_VERSION
    if "collapsed_ontology" not in st.session_state:
        st.session_state.collapsed_ontology = default_collapsed_ontology()
    query_label = st.query_params.get("label")
    if query_label:
        available = set(load_data(DATA_VERSION)["ontology"]["canonical_label"])
        if query_label in available:
            st.session_state.selected_label = query_label
    page = header()
    if page == "Overview":
        overview_page()
    else:
        ontology_page()
    footer()


if __name__ == "__main__":
    main()

