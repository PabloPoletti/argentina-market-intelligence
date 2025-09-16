import subprocess
import pathlib

import nest_asyncio
import streamlit as st

# ——— Debe ser la PRIMERA llamada a Streamlit ———
st.set_page_config(
    page_title="Índice diario IPC‑Online 🇦🇷",
    layout="wide",
)

import duckdb
import pandas as pd
import altair as alt

# Importamos el scraper liviano de ML
from etl.ml_scraper import ml_price_stats
from etl.indexer import update_all_sources, compute_indices

nest_asyncio.apply()  # re‑usa el event‑loop

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

# Si no existe la tabla, la creamos y lanzamos el ETL
tbls = con.execute("SHOW TABLES").fetchall()
if ("prices",) not in tbls:
    update_all_sources(str(DB_PATH))

# ---------- C)  Streamlit UI --------------------------------------------
st.title("Índice Diario de Precios al Consumidor (experimental)")

# Check if we're using demo data
demo_data_check = con.execute("SELECT COUNT(*) FROM prices WHERE source = 'demo_data'").fetchone()[0]
total_records = con.execute("SELECT COUNT(*) FROM prices").fetchone()[0]

if demo_data_check > 0:
    if demo_data_check == total_records:
        st.warning("📊 **MODO DEMOSTRACIÓN**: Mostrando datos sintéticos realistas. Los scrapers están temporalmente inactivos debido a cambios en los sitios web de destino.", icon="🎭")
    else:
        st.info(f"📊 **DATOS MIXTOS**: {demo_data_check} registros sintéticos + {total_records - demo_data_check} registros reales", icon="🔗")

with st.sidebar:
    if st.button("Actualizar precios ahora"):
        update_all_sources(str(DB_PATH))
        st.success("¡Datos actualizados!")

    provincia = st.selectbox(
        "Provincia / Región",
        ["Nacional", "GBA", "Pampeana", "Noreste", "Noroeste", "Cuyo", "Patagonia"],
    )

# ─────────────────────────────────────────────────────────────────────────
#   Carga de datos y cálculo de índice simple (sin diferenciación provincial)
# ─────────────────────────────────────────────────────────────────────────
raw = con.execute("SELECT * FROM prices").fetch_df()
idx = compute_indices(raw)  # nueva firma: solo DataFrame

# ─────────────────────────────────────────────────────────────────────────
#   Gráficos IPC
# ─────────────────────────────────────────────────────────────────────────
st.subheader(f"Evolución general de precios")
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

st.subheader("Divisiones IPC")
div_df = (
    raw.groupby(["division", "date"])
       .price.mean()
       .reset_index()
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

# ─────────────────────────────────────────────────────────────────────────
#   Análisis de Fuentes de Datos y Calidad
# ─────────────────────────────────────────────────────────────────────────
st.subheader("📊 Monitoreo de Fuentes de Datos")

# Mostrar información de las fuentes
try:
    source_health_query = """
    SELECT * FROM source_health 
    ORDER BY timestamp DESC 
    LIMIT 1
    """
    health_data = con.execute(source_health_query).fetchone()
    
    if health_data:
        import json
        health_report = json.loads(health_data[1])
        
        # Create columns for source status
        cols = st.columns(4)
        source_names = ["Coto", "La Anónima", "Jumbo", "MercadoLibre"]
        
        for i, source in enumerate(source_names):
            with cols[i]:
                if source in health_report["sources"]:
                    source_info = health_report["sources"][source]
                    health = source_info["health"]
                    success_rate = source_info["success_rate"]
                    
                    # Color based on health
                    if health == "healthy":
                        emoji = "🟢"
                        color = "green"
                    elif health == "degraded":
                        emoji = "🟡"
                        color = "orange"
                    else:
                        emoji = "🔴"
                        color = "red"
                    
                    st.metric(
                        label=f"{emoji} {source}",
                        value=f"{success_rate:.1%}",
                        help=f"Tasa de éxito: {success_rate:.1%}"
                    )
                else:
                    st.metric(label=f"❓ {source}", value="N/A")
        
        # Overall health indicator
        overall_health = health_report.get("overall_health", "unknown")
        if overall_health == "healthy":
            st.success("🎯 Sistema funcionando óptimamente - Todas las fuentes operativas")
        elif overall_health == "degraded":
            st.warning("⚠️ Sistema funcionando con degradación - Algunas fuentes con problemas")
        else:
            st.error("🚨 Sistema crítico - Múltiples fuentes fallando")
        
except Exception as e:
    st.info("📊 Información de fuentes no disponible (primera ejecución)")

# ─────────────────────────────────────────────────────────────────────────
#   Análisis de Agregación de Precios
# ─────────────────────────────────────────────────────────────────────────
st.subheader("🎯 Análisis de Consenso de Precios")

# Mostrar productos con múltiples fuentes
multi_source_query = """
SELECT name, price, price_sources, num_sources, price_min, price_max, price_std
FROM prices 
WHERE num_sources > 1 AND date = (SELECT MAX(date) FROM prices)
ORDER BY num_sources DESC, name
LIMIT 10
"""

try:
    multi_source_data = con.execute(multi_source_query).fetch_df()
    
    if not multi_source_data.empty:
        st.write("**Productos con consenso de múltiples fuentes:**")
        
        # Format the data for display
        display_df = multi_source_data.copy()
        display_df["price"] = display_df["price"].round(2)
        display_df["price_range"] = display_df.apply(
            lambda row: f"${row['price_min']:.2f} - ${row['price_max']:.2f}" 
            if pd.notna(row['price_min']) and pd.notna(row['price_max']) 
            else "N/A", axis=1
        )
        display_df["reliability"] = display_df["num_sources"].apply(
            lambda x: "🟢 Alta" if x >= 3 else "🟡 Media" if x >= 2 else "🔴 Baja"
        )
        
        # Select columns for display
        display_columns = ["name", "price", "price_range", "num_sources", "price_sources", "reliability"]
        st.dataframe(
            display_df[display_columns].rename(columns={
                "name": "Producto",
                "price": "Precio Consenso ($)",
                "price_range": "Rango de Precios",
                "num_sources": "N° Fuentes",
                "price_sources": "Fuentes",
                "reliability": "Confiabilidad"
            }),
            use_container_width=True
        )
        
        # Visualization of price consensus
        if len(display_df) > 0:
            chart = (
                alt.Chart(display_df)
                .mark_bar()
                .encode(
                    x=alt.X("name:N", title="Producto", sort="-y"),
                    y=alt.Y("price:Q", title="Precio Consenso ($)"),
                    color=alt.Color("num_sources:O", 
                                  title="Fuentes",
                                  scale=alt.Scale(scheme="viridis")),
                    tooltip=["name:N", "price:Q", "num_sources:O", "price_sources:N"]
                )
                .properties(title="Precios de Consenso por Producto")
            )
            st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No se encontraron productos con múltiples fuentes en los datos recientes")
        
except Exception as e:
    st.warning(f"Error al cargar análisis de consenso: {e}")

# ─────────────────────────────────────────────────────────────────────────
#   Comparativo Individual de Mercado Libre
# ─────────────────────────────────────────────────────────────────────────
with st.expander("🔍 Búsqueda Manual en Mercado Libre"):
    st.write("**Herramienta de consulta directa para productos específicos**")
    
    # Manual query input
    manual_query = st.text_input("Buscar producto en Mercado Libre:", placeholder="ej: leche entera, pan lactal")
    
    if manual_query and st.button("Buscar"):
        with st.spinner("Buscando precios en Mercado Libre..."):
            try:
                stats = ml_price_stats(manual_query)
                if stats:
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Precio Promedio", f"${stats['avg_price']:.2f}")
                    with col2:
                        st.metric("Precio Mínimo", f"${stats['min_price']:.2f}")
                    with col3:
                        st.metric("Precio Máximo", f"${stats['max_price']:.2f}")
                else:
                    st.error(f"No se encontraron precios para '{manual_query}' en Mercado Libre")
            except Exception as e:
                st.error(f"Error en la búsqueda: {e}")
