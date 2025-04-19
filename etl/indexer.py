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

def compute_indices(df: pd.DataFrame) -> pd.DataFrame:
    """
    Índice simple global (sin diferenciación provincial).
    Base = primer día disponible, base=100.
    """
    # 1) Agrupo por fecha, precio medio
    daily = (
        df.groupby("date")
          .price.mean()
          .reset_index(name="avg_price")
          .sort_values("date")
    )

    # 2) Si no hay datos, devuelvo DataFrame vacío con columnas esperadas
    if daily.empty:
        return pd.DataFrame(columns=["date", "avg_price", "index"])

    # 3) Base = primer avg_price
    base = daily.iloc[0]["avg_price"]
    if base == 0 or pd.isna(base):
        # Evito división por cero
        daily["index"] = pd.NA
    else:
        daily["index"] = daily["avg_price"] / base * 100

    return daily
