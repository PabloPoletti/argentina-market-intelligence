import pandas as pd

DIV_MAP = {
    "alimentos y bebidas": "Alimentos y bebidas no alcohólicas",
    "despensa": "Alimentos y bebidas no alcohólicas",
    "lácteos": "Leche, lácteos y huevos",
    # ... completar mapping
}

def map_division(raw):
    raw = raw.lower()
    for k, v in DIV_MAP.items():
        if k in raw:
            return v
    return "Otros bienes"

def clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df = df.dropna(subset=["price"])
    df["division"] = df["division"].apply(map_division)
    return df
