# etl/ml_scraper.py

import requests
import pandas as pd
import streamlit as st 

def search_mercadolibre(query: str, limit: int = 50) -> pd.DataFrame | None:
    """
    Busca productos en Mercado Libre utilizando su API pública de búsqueda.
    Args:
        query: término a buscar.
        limit: máximo de resultados por página (50 máximo).
    Returns:
        DataFrame con todos los resultados agregados o None si falla.
    """
    all_items = []
    offset = 0

    while True:
        url = "https://api.mercadolibre.com/sites/MLA/search"
        params = {"q": query, "limit": limit, "offset": offset}
        try:
            r = requests.get(url, params=params, timeout=10)
            r.raise_for_status()
            data = r.json()
            results = data.get("results", [])
            if not results:
                break
            all_items.extend(results)
            offset += limit
        except requests.RequestException as e:
            st.warning(f"Error con ML API para '{query}': {e}")
            return None

    if not all_items:
        return None

    return pd.DataFrame(all_items)

def remove_outliers(df: pd.DataFrame, price_col: str = "price") -> pd.DataFrame:
    """
    Elimina outliers de la columna de precio usando quantiles (10%–90%).
    """
    df = df.copy()
    df[price_col] = pd.to_numeric(df[price_col], errors="coerce")
    df = df.dropna(subset=[price_col])
    q10, q90 = df[price_col].quantile(0.10), df[price_col].quantile(0.90)
    return df[(df[price_col] >= q10) & (df[price_col] <= q90)]

def ml_price_stats(query: str) -> dict[str, float] | None:
    """
    Devuelve estadísticas de precio para una búsqueda en ML:
      - avg_price: precio promedio tras limpieza de outliers
      - min_price, max_price
    """
    df = search_mercadolibre(query)
    if df is None or df.empty:
        return None
    df_clean = remove_outliers(df, "price")
    if df_clean.empty:
        return None
    return {
        "avg_price": df_clean["price"].mean(),
        "min_price": df_clean["price"].min(),
        "max_price": df_clean["price"].max(),
    }
