import duckdb
import pandas as pd


def update_all_sources(db_path="data/prices.duckdb"):
    """
    Ejecuta todo el pipeline de scraping, limpieza y persiste en DuckDB.
    """
    from .scrapers import scrape_all
    from .transform import clean

    # 1) Scrape y limpiar
    df = clean(scrape_all())

    # 2) Conectar y crear tabla si no existe
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
    """
    )

    # 3) Insertar
    con.execute("INSERT INTO prices SELECT * FROM df")


def compute_indices(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula un índice encadenado simple (base = primer día) a partir
    del precio promedio diario de todos los artículos.
    Devuelve DataFrame con columnas: [date, avg_price, index]
    """
    # 1) Precio promedio por día
    daily = (
        df.groupby("date")
          .price.mean()
          .reset_index(name="avg_price")
          .sort_values("date")
    )

    # 2) Base = primer avg_price
    base = daily["avg_price"].iloc[0]

    # 3) Índice (base = 100)
    daily["index"] = daily["avg_price"] / base * 100

    return daily
