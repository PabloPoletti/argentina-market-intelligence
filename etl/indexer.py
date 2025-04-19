import pandas as pd

def compute_indices(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula un índice encadenado simple (base = primer día) a partir
    del precio promedio diario de todos los artículos.
    """
    # 1) Calcular precio promedio por día
    daily = (
        df.groupby("date")
          .price.mean()
          .reset_index(name="avg_price")
          .sort_values("date")
    )

    # 2) Tomar como base el primer día (posición 0 tras el reset_index)
    base = daily["avg_price"].iloc[0]

    # 3) Calcular el índice (base = 100)
    daily["index"] = daily["avg_price"] / base * 100

    return daily
