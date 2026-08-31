"""
dashboard/app.py

Dashboard standalone (fora do Databricks) sobre as tabelas Silver, via
Databricks SQL Warehouse. As agregações (top filmes, gêneros/ano,
comentários/dia, teatros/estado, saúde da pipeline) são calculadas em SQL
na hora, direto sobre `silver.*` e `bronze.control_ingestion_log` — não há
camada Gold intermediária. Complementa notebooks/07_dashboard.py (que roda
dentro do Databricks) com uma versão web independente e compartilhável.

Rodar:
    pip install -r dashboard/requirements.txt
    export DATABRICKS_SERVER_HOSTNAME=...
    export DATABRICKS_HTTP_PATH=...
    export DATABRICKS_TOKEN=...
    streamlit run dashboard/app.py

As três variáveis de ambiente acima nunca são hardcoded nem logadas.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT))
from jobs.common.config import load_collections, load_pipeline_config  # noqa: E402

# Evidência real de execução (saída de query do control_ingestion_log, ver
# docs/evidencias/README.md) — usada como fallback da aba "Saúde da pipeline"
# quando não há credenciais de SQL Warehouse configuradas neste ambiente.
LOCAL_CONTROL_LOG_CSV = REPO_ROOT / "docs" / "evidencias" / "control_ingestion_log.csv"
HAS_DATABRICKS_CREDS = all(
    os.environ.get(v) for v in ("DATABRICKS_SERVER_HOSTNAME", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN")
)

# Paleta categórica validada (ordem fixa — nunca ciclar as cores).
CATEGORICAL = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7", "#e34948"]
BLUE = CATEGORICAL[0]
STATUS = {"good": "#0ca30c", "critical": "#d03b3b", "warning": "#fab219"}
INK_SECONDARY = "#52514e"
GRID = "#e1e0d9"

st.set_page_config(page_title="sample_mflix — Pipeline & Analytics", layout="wide")


@st.cache_resource
def get_connection():
    from databricks import sql

    return sql.connect(
        server_hostname=os.environ["DATABRICKS_SERVER_HOSTNAME"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )


@st.cache_data(ttl=300)
def query(sql_text: str) -> pd.DataFrame:
    conn = get_connection()
    with conn.cursor() as cursor:
        cursor.execute(sql_text)
        return cursor.fetchall_arrow().to_pandas()


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


def stat_tile(col, label: str, value, help_text: str = "") -> None:
    col.metric(label, value, help=help_text)


try:
    pc = load_pipeline_config()
    CATALOG = pc.catalog
    DEFAULT_MIN_VOTES = pc.raw["dashboard"]["top_movies_min_votes"]
    DEFAULT_LIMIT = pc.raw["dashboard"]["top_movies_limit"]
except Exception:
    CATALOG = st.sidebar.text_input("Catalog (Unity Catalog)", value="dev_mflix")
    DEFAULT_MIN_VOTES = 1000
    DEFAULT_LIMIT = 50

st.sidebar.title("sample_mflix")
st.sidebar.caption(f"catalog: `{CATALOG}`")
st.sidebar.caption("lendo direto de `silver.*` (sem camada Gold)")
if not HAS_DATABRICKS_CREDS:
    st.sidebar.caption("saúde da pipeline: evidência local (`docs/evidencias/control_ingestion_log.csv`)")
if st.sidebar.button("🔄 Atualizar dados"):
    st.cache_data.clear()

tab_overview, tab_silver, tab_genres, tab_comments, tab_theaters, tab_ops = st.tabs(
    ["Visão geral", "Dados Silver", "Gêneros & anos", "Comentários", "Teatros", "Saúde da pipeline"]
)

with tab_overview:
    st.subheader("Top filmes por nota IMDb")
    c_votes, c_limit = st.columns(2)
    min_votes = c_votes.slider("Mínimo de votos IMDb", min_value=0, max_value=20000, value=DEFAULT_MIN_VOTES, step=100)
    top_n = c_limit.slider("Quantidade de filmes", min_value=5, max_value=100, value=DEFAULT_LIMIT, step=5)

    top = query(
        f"SELECT _id, title, year, genres, imdb_rating, imdb_votes FROM {CATALOG}.silver.movies "
        f"WHERE imdb_votes >= {min_votes} ORDER BY imdb_rating DESC LIMIT {top_n}"
    )

    c1, c2, c3 = st.columns(3)
    overview = query(
        f"SELECT (SELECT COUNT(*) FROM {CATALOG}.silver.movies) AS n, "
        f"(SELECT COUNT(*) FROM {CATALOG}.silver.comments) AS comments"
    )
    stat_tile(c1, "Filmes na Silver", f"{int(overview['n'][0]):,}")
    stat_tile(c2, "Comentários totais", f"{int(overview['comments'][0]):,}")
    stat_tile(c3, "Filmes no Top (>= min. votos)", f"{len(top):,}")

    if not top.empty:
        top = top.sort_values("imdb_rating")
        fig = go.Figure(
            go.Bar(
                x=top["imdb_rating"],
                y=top["title"],
                orientation="h",
                marker_color=BLUE,
                text=top["imdb_rating"].round(1),
                textposition="outside",
            )
        )
        st.plotly_chart(base_layout(fig, title="Nota IMDb", y_title=""), use_container_width=True)

with tab_silver:
    st.subheader("Cobertura da camada Silver")
    silver_collections = [collection.name for collection in load_collections()]
    silver_summary_sql = " UNION ALL ".join(
        f"SELECT '{collection}' AS collection_name, COUNT(*) AS record_count, "
        f"MAX(_ingestion_timestamp) AS last_ingestion_at FROM {CATALOG}.silver.{collection}"
        for collection in silver_collections
    )
    silver_summary = query(f"SELECT * FROM ({silver_summary_sql}) ORDER BY collection_name")
    st.dataframe(silver_summary, width="stretch", hide_index=True)

    st.subheader("Filmes recentes na Silver")
    recent_movies = query(
        f"SELECT title, year, imdb_rating, imdb_votes, lastupdated_ts "
        f"FROM {CATALOG}.silver.movies ORDER BY lastupdated_ts DESC NULLS LAST LIMIT 30"
    )
    st.dataframe(recent_movies, width="stretch", hide_index=True)

with tab_genres:
    st.subheader("Nota média por gênero ao longo dos anos")
    year_from, year_to = st.slider("Período", min_value=1900, max_value=2020, value=(1970, 2020))
    genre_stats = query(
        f"SELECT genre, year, COUNT(*) AS movie_count, "
        f"ROUND(AVG(imdb_rating), 2) AS avg_imdb_rating, SUM(imdb_votes) AS total_imdb_votes "
        f"FROM {CATALOG}.silver.movies LATERAL VIEW explode(genres) AS genre "
        f"WHERE genres IS NOT NULL AND year BETWEEN {year_from} AND {year_to} "
        f"GROUP BY genre, year"
    )
    if not genre_stats.empty:
        top_genres = (
            genre_stats.groupby("genre")["movie_count"].sum().sort_values(ascending=False).head(8).index.tolist()
        )
        chosen = st.multiselect("Gêneros", options=sorted(genre_stats["genre"].unique()), default=top_genres[:6])
        fig = go.Figure()
        for i, genre in enumerate(chosen):
            sub = genre_stats[genre_stats["genre"] == genre].sort_values("year")
            fig.add_trace(
                go.Scatter(
                    x=sub["year"],
                    y=sub["avg_imdb_rating"],
                    mode="lines",
                    name=genre,
                    line=dict(color=CATEGORICAL[i % len(CATEGORICAL)], width=2),
                )
            )
        st.plotly_chart(base_layout(fig, title="Nota IMDb média por ano", y_title="nota média"), use_container_width=True)
    else:
        st.info("Nenhum filme com gênero/ano no período selecionado.")

with tab_comments:
    st.subheader("Comentários por dia")
    daily = query(
        f"SELECT to_date(date) AS comment_date, COUNT(*) AS comment_count "
        f"FROM {CATALOG}.silver.comments GROUP BY to_date(date) ORDER BY comment_date"
    )
    if not daily.empty:
        fig = go.Figure(
            go.Scatter(x=daily["comment_date"], y=daily["comment_count"], mode="lines", line=dict(color=BLUE, width=2), fill="tozeroy")
        )
        st.plotly_chart(base_layout(fig, title="Volume diário de comentários", y_title="comentários"), use_container_width=True)

with tab_theaters:
    st.subheader("Teatros por estado")
    by_state = query(
        f"SELECT location.address.state AS state, COUNT(*) AS theater_count "
        f"FROM {CATALOG}.silver.theaters GROUP BY location.address.state "
        f"ORDER BY theater_count DESC LIMIT 20"
    )
    if not by_state.empty:
        by_state = by_state.sort_values("theater_count")
        fig = go.Figure(go.Bar(x=by_state["theater_count"], y=by_state["state"], orientation="h", marker_color=BLUE))
        st.plotly_chart(base_layout(fig, title="Quantidade de teatros", y_title=""), use_container_width=True)

with tab_ops:
    st.subheader("Saúde da pipeline (control_ingestion_log)")

    if HAS_DATABRICKS_CREDS:
        health = query(
            f"SELECT collection_name, COUNT(*) AS total_runs, "
            f"SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_runs, "
            f"SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_runs, "
            f"SUM(CASE WHEN status = 'EMPTY' THEN 1 ELSE 0 END) AS empty_runs, "
            f"SUM(qtd_lida_origem) AS total_docs_lidos, MAX(end_time) AS ultima_execucao, "
            f"ROUND(AVG(duration_seconds), 2) AS duracao_media_seg "
            f"FROM {CATALOG}.bronze.control_ingestion_log GROUP BY collection_name"
        )
        runs = query(
            f"SELECT run_id, collection_name, load_type, status, qtd_lida_origem, qtd_gravada_bronze, start_time, end_time "
            f"FROM {CATALOG}.bronze.control_ingestion_log ORDER BY end_time DESC LIMIT 30"
        )
    else:
        st.caption(
            "DATABRICKS_SERVER_HOSTNAME/HTTP_PATH/TOKEN não configurados — mostrando a "
            "evidência real de execução salva em `docs/evidencias/control_ingestion_log.csv` "
            "(3 execuções: full load, incremental sem novidades, incremental com dados novos)."
        )
        control = load_local_control_log()
        health = local_pipeline_health(control)
        runs = control.sort_values("end_time", ascending=False).head(30)[
            ["run_id", "collection_name", "load_type", "status", "qtd_lida_origem", "qtd_gravada_bronze", "start_time", "end_time"]
        ]

    if not health.empty:
        fig = go.Figure()
        fig.add_trace(go.Bar(name="Sucesso", x=health["collection_name"], y=health["success_runs"], marker_color=STATUS["good"]))
        fig.add_trace(go.Bar(name="Falha", x=health["collection_name"], y=health["failed_runs"], marker_color=STATUS["critical"]))
        fig.add_trace(go.Bar(name="Vazia", x=health["collection_name"], y=health["empty_runs"], marker_color=STATUS["warning"]))
        fig.update_layout(barmode="stack")
        st.plotly_chart(base_layout(fig, title="Execuções por status", y_title="execuções"), use_container_width=True)
        st.dataframe(health, use_container_width=True)

    st.subheader("Últimas execuções")
    st.dataframe(runs, use_container_width=True)
