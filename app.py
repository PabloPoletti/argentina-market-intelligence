--- a/app.py
+++ b/app.py
@@
 import altair as alt
+from etl.ml_scraper import ml_price_stats

 nest_asyncio.apply()

@@
 st.title("Índice Diario de Precios al Consumidor (experimental)")

 with st.sidebar:
     if st.button("Actualizar precios ahora"):
         from etl.indexer import update_all_sources
         update_all_sources(str(DB_PATH))
         st.success("¡Datos actualizados!")

     provincia = st.selectbox(
@@
 raw = con.execute("SELECT * FROM prices").fetch_df()
 raw = raw[
     (raw["province"] == provincia)
     | (raw["province"] == "Nacional")
 ]

 idx = compute_indices(raw, weights_path="data/weights.csv")

+# ─────────────────────────────────────────────────────────────────────────
+#   Sección Mercado Libre
+# ─────────────────────────────────────────────────────────────────────────
+st.subheader("Comparativo de precios en Mercado Libre")
+
+# Lista de queries de ejemplo (puedes cargarla desde CSV o definirla aquí)
+queries = ["leche entera", "pan francés", "aceite girasol"]
+ml_stats = {}
+for q in queries:
+    stats = ml_price_stats(q)
+    if stats:
+        ml_stats[q] = stats
+
+if ml_stats:
+    ml_df = pd.DataFrame(ml_stats).T.reset_index().rename(columns={
+        "index": "producto",
+        "avg_price": "Precio promedio ML",
+        "min_price": "Precio mínimo ML",
+        "max_price": "Precio máximo ML",
+    })
+    st.dataframe(ml_df, use_container_width=True)
+    # Gráfico de barras comparando promedio vs. mínimo/máximo
+    chart = (
+        alt.Chart(ml_df.melt(id_vars="producto", 
+                             value_vars=["Precio promedio ML","Precio mínimo ML","Precio máximo ML"]))
+           .mark_bar()
+           .encode(
+               x="producto:N",
+               y="value:Q",
+               color="variable:N",
+               tooltip=["producto:N", "variable:N", "value:Q"]
+           )
+    )
+    st.altair_chart(chart, use_container_width=True)
+else:
+    st.info("No se obtuvieron datos de Mercado Libre para las consultas definidas.")
+
 # ─────────────────────────────────────────────────────────────────────────
 #   Gráficos IPC
 # ─────────────────────────────────────────────────────────────────────────
