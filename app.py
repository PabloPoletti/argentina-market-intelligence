# ───────────────────────────────────────────────────────────────
#  app.py
# ───────────────────────────────────────────────────────────────
import subprocess
import pathlib
import nest_asyncio
import streamlit as st

# Debe ser la PRIMERA llamada a Streamlit
st.set_page_config(
    page_title="Índice Diario IPC‑Online 🇦🇷",
    layout="wide",
)

import duckdb
import pandas as pd
import altair as alt

# Reusar event-loop en entornos asíncronos
nest_asyncio.apply()

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

# Si no existe tabla, ejecutar primer ETL
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
#   Carga de datos y cálculo de índice global simple
# ─────────────────────────────────────────────────────────────────────────
from etl.indexer import compute_indices

raw = con.execute("SELECT * FROM prices").fetch_df()
# Eliminamos diferenciación provincial: usamos todos los datos
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
           tooltip=["date:T", "avg_price:Q", "index:Q"],
       ),
    use_container_width=True,
)

st.subheader("Precio promedio diario")
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
