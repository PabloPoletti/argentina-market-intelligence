# etl/indexer.py

import duckdb
import pandas as pd

def update_all_sources(db_path="data/prices.duckdb"):
    from .scrapers import scrape_all
    from .transform import clean

    df = clean(scrape_all())

    con = duckdb.connect(db_path)
    con.execute("""
        CREATE TABLE IF NOT EXISTS prices (
            date        DATE,
            store       VARCHAR,
            sku         VARCHAR,
            name        VARCHAR,
            price       DOUBLE,
            division    VARCHAR,
            province    VARCHAR
        )
    """)
    con.execute("INSERT INTO prices SELECT * FROM df")


def compute_indices(df: pd.DataFrame, weights_path=None):
    """
    Índice de precios simple:
      index_t = (precio_promedio_t / precio_promedio_base) * 100
    """
    # 1) Agrupo por fecha y calcúlo precio promedio
    daily = (
        df
        .groupby("date", as_index=False)
        .price
        .mean()
        .rename(columns={"price": "avg_price"})
    )

    # 2) Tomo como base el primer día
    base = daily.loc[0, "avg_price"]

    # 3) Calculo el índice
    daily["index"] = daily["avg_price"] / base * 100

    # 4) Devuelvo solo fecha + índice
    return daily[["date", "index"]]
