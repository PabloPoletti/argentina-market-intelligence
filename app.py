# ──────────────────────────────  app.py  ─────────────────────────────
import subprocess, pathlib, os, asyncio, nest_asyncio
import streamlit as st
import duckdb, pandas as pd, altair as alt

# ---------- A) asegurar Playwright ------------------------------------------------
def ensure_playwright():
    cache_dir = pathlib.Path.home() / ".cache" / "ms-playwright"
    if not cache_dir.exists():                       # solo la 1.ª vez
        st.info("Descargando navegadores Playwright… (≈30 s)")
        subprocess.run(
            ["playwright", "install", "chromium", "--with-deps"],
            check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )
ensure_playwright()                                  # se ejecuta antes de usar scrapers

# ---------- B) base de datos DuckDB ------------------------------------------------
DB_PATH = pathlib.Path("data/prices.duckdb")
DB_PATH.parent.mkdir(exist_ok=True)                  # crea /data si falta
con = duckdb.connect(str(DB_PATH))                   # sin read_only=True

# si la base está vacía la rellenamos en caliente
if not DB_PATH.exists() or not con.execute(
        "SELECT * FROM duckdb_tables() WHERE name='prices'").fetchall():
    from etl.indexer import update_all_sources
    update_all_sources(str(DB_PATH))

# ---------- C) configuración de la página ------------------------------------------
st.set_page_config(page_title="Índice diario IPC‑Online 🇦🇷", layout="wide")
st.title("Índice Diario de Precios al Consumidor (experimental)")

# ---------- D) barra lateral -------------------------------------------------------
with st.sidebar:
    st.subheader("Controles")
    if st.button("Actualizar precios ahora"):
        from etl.indexer import update_all_sources
        update_all_sources(str(DB_PATH))
        st.success("Datos extraídos y cargados")

    provincia = st.selectbox(
        "Provincia / Región",
        ["Nacional", "GBA", "Pampeana", "Noreste", "Noroeste", "Cuyo", "Patagonia"]
    )

# ---------- E) consulta + cálculo del índice --------------------------------------
from etl.indexer import compute_indices
raw = con.execute("SELECT * FROM prices").fetch_df()
raw = raw[(raw["province"] == provincia) | (raw["province"] == "Nacional")]

idx = compute_indices(raw)

# ---------- F) visualización -------------------------------------------------------
st.subheader(f"Evolución {provincia}")
line = (
    alt.Chart(idx).mark_line().encode(
        x="date:T", y="index:Q",
        tooltip=["date:T", "index:Q"]
    )
)
st.altair_chart(line, use_container_width=True)

st.subheader("Divisiones IPC")
divisiones = (
    raw.groupby(["division", "date"]).price.mean().reset_index()
)
chart = (
    alt.Chart(divisiones).mark_line().encode(
        x="date:T", y="price:Q", color="division:N",
        tooltip=["division:N", "price:Q", "date:T"]
    )
)
st.altair_chart(chart, use_container_width=True)
# ────────────────────────────────────────────────────────────────────
