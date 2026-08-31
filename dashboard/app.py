"""
dashboard/app.py

Dashboard standalone (fora do Databricks) sobre a saúde da pipeline de
ingestão. Lê a evidência local de execução salva em
docs/evidencias/control_ingestion_log.csv — sem conexão a um Databricks
SQL Warehouse e sem consultas às tabelas Silver.

Rodar:
    pip install -r dashboard/requirements.txt
    streamlit run dashboard/app.py
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

REPO_ROOT = Path(__file__).resolve().parents[1]

# Evidência real de execução (saída de query do control_ingestion_log, ver
# docs/evidencias/README.md).
LOCAL_CONTROL_LOG_CSV = REPO_ROOT / "docs" / "evidencias" / "control_ingestion_log.csv"

STATUS = {"good": "#0ca30c", "critical": "#d03b3b", "warning": "#fab219"}
GRID = "#e1e0d9"

st.set_page_config(page_title="sample_mflix — Saúde da pipeline", layout="wide")


@st.cache_data(ttl=300)
def load_local_control_log() -> pd.DataFrame:
    """Lê a evidência local do control_ingestion_log e normaliza os nomes de
    coluna para o mesmo formato usado nas consultas ao SQL Warehouse."""
    df = pd.read_csv(LOCAL_CONTROL_LOG_CSV)
    df = df.rename(
        columns={
            "_ingestion_id": "run_id",
            "collection": "collection_name",
            "watermark_inicial": "watermark_start",
            "watermark_final": "watermark_end",
            "qtd_gravada_destino": "qtd_gravada_bronze",
            "duracao_seg": "duration_seconds",
            "mensagem_erro": "error_message",
        }
    )
    df["start_time"] = pd.to_datetime(df["start_time"])
    df["end_time"] = pd.to_datetime(df["end_time"])
    return df


def local_pipeline_health(control: pd.DataFrame) -> pd.DataFrame:
    health = control.groupby("collection_name").agg(
        total_runs=("run_id", "count"),
        success_runs=("status", lambda s: (s == "SUCCESS").sum()),
        failed_runs=("status", lambda s: (s == "FAILED").sum()),
        empty_runs=("status", lambda s: (s == "EMPTY").sum()),
        total_docs_lidos=("qtd_lida_origem", "sum"),
        ultima_execucao=("end_time", "max"),
        duracao_media_seg=("duration_seconds", lambda s: round(s.mean(), 2)),
    )
    return health.reset_index()


def base_layout(fig: go.Figure, *, title: str, y_title: str = "") -> go.Figure:
    fig.update_layout(
        title=title,
        paper_bgcolor="#fcfcfb",
        plot_bgcolor="#fcfcfb",
        font=dict(color="#0b0b0b", family="system-ui, -apple-system, 'Segoe UI', sans-serif"),
        yaxis=dict(title=y_title, gridcolor=GRID, zeroline=False),
        xaxis=dict(title="", gridcolor=GRID, zeroline=False),
        legend=dict(orientation="h", y=-0.2),
        margin=dict(t=48, l=8, r=8, b=8),
    )
    return fig


st.title("sample_mflix — Saúde da pipeline")
st.caption(
    "Evidência local de execução salva em "
    "`docs/evidencias/control_ingestion_log.csv`."
)

if st.sidebar.button("🔄 Atualizar dados"):
    st.cache_data.clear()

control = load_local_control_log()
health = local_pipeline_health(control)
runs = control.sort_values("end_time", ascending=False).head(30)[
    [
        "run_id",
        "collection_name",
        "load_type",
        "status",
        "qtd_lida_origem",
        "qtd_gravada_bronze",
        "start_time",
        "end_time",
    ]
]

if not health.empty:
    fig = go.Figure()
    fig.add_trace(
        go.Bar(name="Sucesso", x=health["collection_name"], y=health["success_runs"], marker_color=STATUS["good"])
    )
    fig.add_trace(
        go.Bar(name="Falha", x=health["collection_name"], y=health["failed_runs"], marker_color=STATUS["critical"])
    )
    fig.add_trace(
        go.Bar(name="Vazia", x=health["collection_name"], y=health["empty_runs"], marker_color=STATUS["warning"])
    )
    fig.update_layout(barmode="stack")
    st.plotly_chart(base_layout(fig, title="Execuções por status", y_title="execuções"), use_container_width=True)
    st.dataframe(health, use_container_width=True)

st.subheader("Últimas execuções")
st.dataframe(runs, use_container_width=True)