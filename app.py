import streamlit as st, duckdb, pandas as pd, altair as alt
from etl.indexer import update_all_sources, compute_indices

st.set_page_config(page_title="Índice Diario IPC‑Online 🇦🇷", layout="wide")
st.title("Índice diario de precios al consumidor (experimental)")

# ────────── barra lateral ──────────
with st.sidebar:
    if st.button("Actualizar ahora"):
        update_all_sources()
        st.success("¡Datos extraídos!")

    prov = st.selectbox(
        "Provincia / Región",
        options=["Nacional", "GBA", "Pampeana", "Noreste", "Noroeste", "Cuyo", "Patagonia"],
        index=0
    )

# ────────── datos ──────────
con = duckdb.connect("data/prices.duckdb", read_only=True)
raw = con.execute("SELECT * FROM prices").fetch_df()
raw = raw[raw["province"].eq(prov) | raw["province"].eq("Nacional")]

# ────────── índice ──────────
idx = compute_indices(raw)
st.subheader(f"Evolución {prov}")
line = (
    alt.Chart(idx)
    .mark_line()
    .encode(
        x="date:T",
        y="index:Q",
        tooltip=["date:T", "index:Q"]
    )
)
st.altair_chart(line, use_container_width=True)

# ────────── detalle divisional ──────────
st.subheader("Divisiones IPC")
top = raw.groupby(["division", "date"]).price.mean().reset_index()
div_chart = (
    alt.Chart(top)
       .mark_line()
       .encode(
            x="date:T", y="price:Q",
            color="division:N",
            tooltip=["division:N", "price:Q", "date:T"]
       )
)
st.altair_chart(div_chart, use_container_width=True)
