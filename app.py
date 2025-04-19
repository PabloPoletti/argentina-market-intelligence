# app.py

import subprocess
import pathlib

import nest_asyncio
import streamlit as st           # ← import de Streamlit
nest_asyncio.apply()            # re‑usa el event‑loop

# ——— PRIMErA llamada a Streamlit ———
st.set_page_config(
    page_title="Índice diario IPC‑Online 🇦🇷",
    layout="wide",
)

import duckdb
import pandas as pd
import altair as alt

# ---------- A)  Garantizar Chromium (Playwright) -------------------------
def ensure_playwright():
    cache = pathlib.Path.home() / ".cache" / "ms-playwright" / "chromium"
    if cache.exists():
        return
    st.info("Descargando Chromium… (sólo la primera vez)")
    subprocess.run(["playwright", "install", "chromium"], check=True)

ensure_playwright()

# ---------- B)  Base DuckDB ---------------------------------------------
DB_PATH = pathlib.Path("data/prices.duckdb")
DB_PATH.parent.mkdir(exist_ok=True)
con = duckdb.connect(str(DB_PATH))

# ▸ Si no existe la tabla, la creo y tiro el ETL
tbls = con.execute("SHOW TABLES").fetchall()
if ("prices",) not in tbls:
    from etl.indexer import update_all_sources
    update_all_sources(str(DB_PATH))

# ---------- C)  Streamlit UI --------------------------------------------
st.title("Índice Diario de Precios al Consumidor (experimental)")

with st.sidebar:
    if st.button("Actualizar precios ahora"):
        from etl.indexer import update_all_sources
        update_all_sources(str(DB_PATH))
        st.success("¡Datos actualizados!")

# ─────────────────────────────────────────────────────────────────────────
#   Carga de datos y cálculos
# ─────────────────────────────────────────────────────────────────────────
from etl.indexer import compute_indices

# 1) Cargo todo el histórico de precios (sin filtrar por province)
raw = con.execute("SELECT * FROM prices").fetch_df()

# 2) Calculo el índice simple
idx = compute_indices(raw)

# ─────────────────────────────────────────────────────────────────────────
#   Gráficos
# ─────────────────────────────────────────────────────────────────────────
st.subheader("Evolución del índice de precios")
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

st.subheader("Divisiones IPC (precio promedio)")
div_df = (
    raw
    .groupby(["division", "date"], as_index=False)
    .price
    .mean()
)
st.altair_chart(
    alt.Chart(div_df)
       .mark_line()
       .encode(
           x="date:T",
           y="price:Q",
           color="division:N",
           tooltip=["division:N", "price:Q", "date:T"],
       ),
    use_container_width=True,
)
