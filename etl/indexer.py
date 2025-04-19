import duckdb, pandas as pd

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

def compute_indices(df: pd.DataFrame, weights_path="weights.csv"):
    """Laspeyres encadenado base Dic‑24=100"""
    w = pd.read_csv(weights_path)
    merged = df.merge(w, on=["division", "province"], how="left")
    basket = (merged.groupby(["division", "province"])
                     .apply(lambda x: (x["price"] * x["weight"]).sum()))
    basket = basket.groupby(level=[1]).sum()  # índice provincial
    base = basket.loc[basket.index.min()]     # dic‑24
    idx = (basket / base * 100).reset_index()
    idx.rename(columns={0: "index"}, inplace=True)
    return idx
