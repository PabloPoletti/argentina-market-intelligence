# ──────────────────────────────  app.py  ─────────────────────────────
import subprocess, pathlib, asyncio, nest_asyncio
import streamlit as st
import duckdb, pandas as pd, altair as alt

nest_asyncio.apply()                       # ← evita colisión de loops

# ---------- A) descarga de navegadores Playwright -------------------
def ensure_playwright():
    """Descarga Chromium si falta, sin usar --with-deps (requiere sudo)."""
    cache_dir = pathlib.Path.home() / ".cache" / "ms-playwright" / "chromium"
    if cache_dir.exists():        # ya está descargado
        return
    st.info("Descargando Chromium (Playwright)… primera vez ≈30 s")
    try:
        subprocess.run(
            ["playwright", "install", "chromium"],      # sin --with-deps
            check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )
    except subprocess.CalledProcessError as e:
        # Mostramos aviso pero dejamos seguir: si falla aquí fallará el scraper
        st.error("No se pudo descargar Chromium. Reintenta el deploy o "
                 "revisa tu archivo packages.txt.")
        raise

ensure_playwright()                      # se ejecuta antes de usar los scrapers

# ---------- B) base de datos DuckDB ---------------------------------
DB_PATH = pathlib.Path("data/prices.duckdb")
DB_PATH.parent.mkdir(exist_ok=True)
con = duckdb.connect(str(DB_PATH))

# crea la tabla la primera vez
if not DB_PATH.exists() or not con.execute(
        "SELECT * FROM duckdb_tables() WHERE name='prices'").fetchall():
    from etl.indexer import update_all_sources
    update_all_sources(str(DB_PATH))

# ---------- C) configuración de la página ---------------------------
st.set_page_config(page_title="Índice diario IPC‑Online 🇦🇷", layout="wide")
st.title("Índice Diario de Precios al Consumidor (experimental)")

# ---------- D) barra lateral ----------------------------------------
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

# ---------- E) datos + índice ---------------------------------------
from etl.indexer import compute_indices
raw = con.execute("SELECT * FROM prices").fetch_df()
raw = raw[(raw["province"] == provincia) | (raw["province"] == "Nacional")]

idx = compute_indices(raw)

# ---------- F) visualización ----------------------------------------
st.subheader(f"Evolución {provincia}")
st.altair_chart(
    alt.Chart(idx).mark_line().encode(
        x="date:T", y="index:Q", tooltip=["date:T", "index:Q"]
    ),
    use_container_width=True
)

st.subheader("Divisiones IPC")
div_df = raw.groupby(["division", "date"]).price.mean().reset_index()
st.altair_chart(
    alt.Chart(div_df).mark_line().encode(
        x="date:T", y="price:Q", color="division:N",
        tooltip=["division:N", "price:Q", "date:T"]
    ),
    use_container_width=True
)
# ────────────────────────────────────────────────────────────────────
