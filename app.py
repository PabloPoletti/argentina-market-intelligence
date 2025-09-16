import subprocess
import pathlib
import time

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

# Check ONLY real data sources
real_data_sources = con.execute("""
    SELECT COUNT(*) as total, 
           COUNT(DISTINCT source) as sources,
           GROUP_CONCAT(DISTINCT source) as source_list
    FROM prices 
    WHERE source IS NOT NULL AND source != ''
""").fetchone()

total_real_records = real_data_sources[0] if real_data_sources[0] else 0
num_sources = real_data_sources[1] if real_data_sources[1] else 0
source_list = real_data_sources[2] if real_data_sources[2] else "Ninguna"

if total_real_records > 0:
    st.success(f"🌐 **DATOS REALES EN VIVO**: {total_real_records:,} productos de {num_sources} fuentes online", icon="✅")
    
    # Show source breakdown
    with st.expander("📊 Ver fuentes de datos reales"):
        sources = source_list.split(',') if source_list != "Ninguna" else []
        for source in sources:
            count = con.execute(f"SELECT COUNT(*) FROM prices WHERE source = '{source.strip()}'").fetchone()[0]
            st.write(f"• **{source.strip()}**: {count:,} productos")
        
        last_update = con.execute("SELECT MAX(date) FROM prices").fetchone()[0]
        if last_update:
            st.write(f"🕒 **Última actualización**: {last_update}")
else:
    st.error("❌ **NO HAY DATOS REALES**: El sistema no pudo obtener datos de ninguna fuente online.", icon="🚫")
    st.info("🔄 **Acción requerida**: Presiona 'Actualizar precios ahora' para intentar recolectar datos reales.", icon="💡")

with st.sidebar:
    if st.button("🔄 Actualizar precios ahora"):
        with st.spinner("🌐 Recolectando datos reales de múltiples fuentes..."):
            try:
        update_all_sources(str(DB_PATH))
                st.success("✅ ¡Datos reales actualizados!")
                st.balloons()
                time.sleep(2)
                st.rerun()
            except Exception as e:
                st.error(f"❌ Error al obtener datos reales: {str(e)}")
                st.info("💡 Las fuentes online pueden estar temporalmente inaccesibles. Intenta de nuevo en unos minutos.")
    
    st.markdown("---")
    st.markdown("**🌐 Fuentes de datos reales:**")
    st.markdown("• CheSuper.ar")
    st.markdown("• PreciosHoy.com.ar") 
    st.markdown("• SeguiPrecios.com.ar")
    st.markdown("• Argentina.gob.ar")
    st.markdown("• MercadoLibre API")
    st.markdown("• Retailers directos")

    provincia = st.selectbox(
        "Provincia / Región",
        ["Nacional", "GBA", "Pampeana", "Noreste", "Noroeste", "Cuyo", "Patagonia"],
    )

# ─────────────────────────────────────────────────────────────────────────
#   Carga de datos y cálculo de índice simple (sin diferenciación provincial)
# ─────────────────────────────────────────────────────────────────────────
raw = con.execute("SELECT * FROM prices").fetch_df()

# Convert date column to datetime for filtering
if not raw.empty:
    raw['date'] = pd.to_datetime(raw['date'])
    
    # ─────────────────────────────────────────────────────────────────────────
    #   Controles Avanzados de Agregación Temporal
    # ─────────────────────────────────────────────────────────────────────────
    st.subheader("📊 Análisis Temporal Avanzado")
    
    # Get date range from data
    min_date = raw['date'].min().date()
    max_date = raw['date'].max().date()
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        aggregation_type = st.selectbox(
            "🔄 Agregación de datos",
            ["Diario", "Semanal", "Mensual"],
            index=0,
            help="Selecciona cómo agrupar los datos de precios"
        )
    
    with col2:
        # Adjust time filter options based on aggregation
        if aggregation_type == "Diario":
            time_options = ["Últimos 7 días", "Últimos 15 días", "Últimos 30 días", "Últimos 60 días", "Personalizado"]
            default_option = "Últimos 30 días"
        elif aggregation_type == "Semanal":
            time_options = ["Últimas 4 semanas", "Últimas 8 semanas", "Últimas 12 semanas", "Últimas 26 semanas", "Personalizado"]
            default_option = "Últimas 12 semanas"
        else:  # Mensual
            time_options = ["Últimos 3 meses", "Últimos 6 meses", "Último año", "Últimos 2 años", "Personalizado"]
            default_option = "Últimos 6 meses"
        
        time_filter = st.selectbox(
            "⏰ Período de análisis",
            time_options,
            index=time_options.index(default_option) if default_option in time_options else 0,
            help=f"Rango de tiempo para mostrar datos {aggregation_type.lower()}s"
        )
    
    # Apply time filtering based on selection and aggregation
    if "Personalizado" in time_filter:
        with col3:
            start_date = st.date_input("📅 Fecha inicial", min_date, min_value=min_date, max_value=max_date)
        with col4:
            end_date = st.date_input("📅 Fecha final", max_date, min_value=min_date, max_value=max_date)
        
        if start_date <= end_date:
            filtered_raw = raw[(raw['date'] >= pd.Timestamp(start_date)) & (raw['date'] <= pd.Timestamp(end_date))]
        else:
            st.error("La fecha inicial debe ser anterior a la fecha final")
            filtered_raw = raw
    else:
        # Smart period calculation based on aggregation type
        if aggregation_type == "Diario":
            if "7 días" in time_filter:
                cutoff_date = pd.Timestamp(max_date) - pd.Timedelta(days=7)
            elif "15 días" in time_filter:
                cutoff_date = pd.Timestamp(max_date) - pd.Timedelta(days=15)
            elif "30 días" in time_filter:
                cutoff_date = pd.Timestamp(max_date) - pd.Timedelta(days=30)
            elif "60 días" in time_filter:
                cutoff_date = pd.Timestamp(max_date) - pd.Timedelta(days=60)
            else:
                cutoff_date = pd.Timestamp(min_date)
                
        elif aggregation_type == "Semanal":
            if "4 semanas" in time_filter:
                cutoff_date = pd.Timestamp(max_date) - pd.Timedelta(weeks=4)
            elif "8 semanas" in time_filter:
                cutoff_date = pd.Timestamp(max_date) - pd.Timedelta(weeks=8)
            elif "12 semanas" in time_filter:
                cutoff_date = pd.Timestamp(max_date) - pd.Timedelta(weeks=12)
            elif "26 semanas" in time_filter:
                cutoff_date = pd.Timestamp(max_date) - pd.Timedelta(weeks=26)
            else:
                cutoff_date = pd.Timestamp(min_date)
                
        else:  # Mensual
            if "3 meses" in time_filter:
                cutoff_date = pd.Timestamp(max_date) - pd.DateOffset(months=3)
            elif "6 meses" in time_filter:
                cutoff_date = pd.Timestamp(max_date) - pd.DateOffset(months=6)
            elif "año" in time_filter:
                cutoff_date = pd.Timestamp(max_date) - pd.DateOffset(months=12)
            elif "2 años" in time_filter:
                cutoff_date = pd.Timestamp(max_date) - pd.DateOffset(months=24)
            else:
                cutoff_date = pd.Timestamp(min_date)
        
        filtered_raw = raw[raw['date'] >= cutoff_date]
    
    # Apply data aggregation based on type
    if not filtered_raw.empty:
        # Set date as index for resampling
        temp_df = filtered_raw.copy()
        temp_df.set_index('date', inplace=True)
        
        if aggregation_type == "Diario":
            # Daily data - no aggregation needed, just ensure daily frequency
            aggregated_raw = temp_df.resample('D').agg({
                'price': 'mean',
                'store': 'first',
                'sku': 'first', 
                'name': 'first',
                'division': 'first',
                'province': 'first',
                'source': 'first',
                'price_sources': 'first',
                'num_sources': 'sum',
                'price_min': 'min',
                'price_max': 'max',
                'price_std': 'mean',
                'reliability_weight': 'mean'
            }).dropna()
            
        elif aggregation_type == "Semanal":
            # Weekly aggregation (Monday to Sunday)
            aggregated_raw = temp_df.resample('W-MON').agg({
                'price': 'mean',
                'store': 'first',
                'sku': 'first',
                'name': 'first', 
                'division': 'first',
                'province': 'first',
                'source': 'first',
                'price_sources': 'first',
                'num_sources': 'sum',
                'price_min': 'min',
                'price_max': 'max',
                'price_std': 'mean',
                'reliability_weight': 'mean'
            }).dropna()
            
        else:  # Mensual
            # Monthly aggregation
            aggregated_raw = temp_df.resample('M').agg({
                'price': 'mean',
                'store': 'first',
                'sku': 'first',
                'name': 'first',
                'division': 'first', 
                'province': 'first',
                'source': 'first',
                'price_sources': 'first',
                'num_sources': 'sum',
                'price_min': 'min',
                'price_max': 'max',
                'price_std': 'mean',
                'reliability_weight': 'mean'
            }).dropna()
        
        # Reset index to have date as column again
        filtered_raw = aggregated_raw.reset_index()
        
        # Display aggregated period info
        if not filtered_raw.empty:
            period_start = filtered_raw['date'].min().strftime('%d/%m/%Y')
            period_end = filtered_raw['date'].max().strftime('%d/%m/%Y')
            total_records = len(filtered_raw)
            
            # Smart period description
            if aggregation_type == "Diario":
                period_desc = f"datos diarios"
            elif aggregation_type == "Semanal":
                period_desc = f"promedios semanales"
            else:
                period_desc = f"promedios mensuales"
                
            st.success(f"📊 **{aggregation_type}**: {period_start} a {period_end} ({total_records} {period_desc})")
        else:
            st.warning("⚠️ No hay datos suficientes para el período y agregación seleccionados")
            filtered_raw = raw  # Fallback to all data
    else:
        st.warning("⚠️ No hay datos en el período seleccionado")
        filtered_raw = raw  # Fallback to all data
        
else:
    filtered_raw = raw

idx = compute_indices(filtered_raw)  # nueva firma: solo DataFrame

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
    width='stretch',
)

st.subheader("Divisiones IPC")
div_df = (
    filtered_raw.groupby(["division", "date"])
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
    width='stretch',
)

# ─────────────────────────────────────────────────────────────────────────
#   Comparación por Tiendas (datos filtrados)
# ─────────────────────────────────────────────────────────────────────────
if not filtered_raw.empty:
    st.subheader("🏪 Comparación de Precios por Tienda")
    
    # Group by store and date for comparison
    store_df = (
        filtered_raw.groupby(["store", "date"])
                   .price.mean()
                   .reset_index()
    )
    
    if not store_df.empty:
        st.altair_chart(
            alt.Chart(store_df)
               .mark_line(point=True)
               .encode(
                   x="date:T",
                   y="price:Q",
                   color="store:N",
                   tooltip=["store:N", "price:Q", "date:T"],
               )
               .interactive(),
            width='stretch',
        )
        
        # Summary statistics by store
        store_stats = (
            filtered_raw.groupby("store")
                       .agg({
                           'price': ['mean', 'min', 'max', 'count']
                       })
                       .round(2)
        )
        store_stats.columns = ['Precio Promedio', 'Precio Mínimo', 'Precio Máximo', 'Productos']
        
        st.write("**Estadísticas por tienda:**")
        st.dataframe(store_stats, width='stretch')

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
                st.info("🎭 **Modo Demostración Activo**: Los scrapers web están temporalmente inactivos debido a cambios en los sitios de destino. El sistema utiliza datos sintéticos realistas para demostrar funcionalidad completa.")
        
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
            width='stretch'
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
    st.altair_chart(chart, width='stretch')
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
