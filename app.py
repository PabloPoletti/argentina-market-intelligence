# ───────────────────────────────────────────────────────────────
#  app.py
# ───────────────────────────────────────────────────────────────
import subprocess
import pathlib
import nest_asyncio
import streamlit as st

# — Debe ser la primera llamada a Streamlit:
st.set_page_config(
    page_title="Índice diario IPC‑Online 🇦🇷",
    layout="wide",
)

import duckdb
import pandas as pd
import altair as alt

nest_asyncio.apply()  # re‑usa el event‑loop

# ---------- A) Garantizar Chromium (Playwright) -------------------------
def ensure_playwright():
    cache = pathlib.Path.home() / ".cache" / "ms-playwright" / "chromium"
    if cache.exists():
        return
    st.info("Descargando Chromium… (sólo la primera vez)")
    subprocess.run(["playwright", "install", "chromium"], check=True)

ensure_playwright()

# ---------- B) Base DuckDB ---------------------------------------------
DB_PATH = pathlib.Path("data/prices.duckdb")
DB_PATH.parent.mkdir(exist_ok=True)
con = duckdb.connect(str(DB_PATH))

# Si no existe la tabla, la creo y tiro el ETL
tbls = con.execute("SHOW TABLES").fetchall()
if ("prices",) not in tbls:
    from etl.indexer import update_all_sources
    update_all_sources(str(DB_PATH))

# ---------- C) Streamlit UI --------------------------------------------
st.title("Índice Diario de Precios al Consumidor (experimental)")

if st.sidebar.button("Actualizar precios ahora"):
    from etl.indexer import update_all_sources
    update_all_sources(str(DB_PATH))
    st.success("¡Datos actualizados!")

# ─────────────────────────────────────────────────────────────────────────
#   Carga de datos y cálculos (sin diferenciación provincial)
# ─────────────────────────────────────────────────────────────────────────
from etl.indexer import compute_indices

raw = con.execute("SELECT * FROM prices").fetch_df()

idx = compute_indices(raw)

if idx.empty:
    st.warning("No hay datos suficientes para calcular el índice.")
else:
    # ─────────────────────────────────────────────────────────────────────────
    #   Gráficos
    # ─────────────────────────────────────────────────────────────────────────
    st.subheader("Evolución Índice de Precios")
    st.altair_chart(
        alt.Chart(idx)
           .mark_line()
           .encode(
               x="date:T",
               y="index:Q",
               tooltip=["date:T", "index:Q"],
           ),
        use_container_width=True,
    )

    st.subheader("Precio Medio Diario")
    st.altair_chart(
        alt.Chart(idx)
           .mark_line()
           .encode(
               x="date:T",
               y="avg_price:Q",
               tooltip=["date:T", "avg_price:Q"],
           ),
        use_container_width=True,
    )
