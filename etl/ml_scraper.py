# etl/ml_scraper.py

import requests
from bs4 import BeautifulSoup
import numpy as np

def ml_price_stats(query: str, limit: int = 20):
    """
    Devuelve un dict con avg_price, min_price y max_price para la búsqueda 'query'
    en MercadoLibre, extrayendo precios de la página HTML.
    """
    # Cabeceras para simular un navegador real
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36"
        )
    }
    # Construimos la URL pública de búsqueda
    safe_query = query.replace(" ", "-")
    url = f"https://listado.mercadolibre.com.ar/{safe_query}"
    
    resp = requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    
    soup = BeautifulSoup(resp.text, "html.parser")
    
    # Seleccionamos las fracciones de precio visibles en los resultados
    price_tags = soup.select(
        "li.ui-search-layout__item .ui-search-price__second-line .price-tag-fraction"
    )
    prices = []
    for tag in price_tags[:limit]:
        text = tag.get_text().strip().replace(".", "")
        try:
            prices.append(float(text))
        except ValueError:
            continue
    
    if not prices:
        return None
    
    arr = np.array(prices, dtype=float)
    return {
        "avg_price": float(arr.mean()),
        "min_price": float(arr.min()),
        "max_price": float(arr.max()),
    }
