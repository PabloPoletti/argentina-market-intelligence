# ────────────────────  etl/scrapers.py  ─────────────────────────────
import json
import re
import asyncio
from datetime import date
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
import nest_asyncio; nest_asyncio.apply()  # evita error de event‑loop

from playwright.async_api import async_playwright

# ──────────────── SOLO ASCII EN ESTAS CABECERAS ─────────────────
HEADERS = {
    "User-Agent": "Mozilla/5.0 (price-index-bot 1.0)",
    "Accept":     "application/json; charset=UTF-8"
}

# ---------- util de división -------------------------------------------------------
def map_division(raw):
    raw = str(raw).lower()
    if "lácte" in raw or "leche" in raw:
        return "Leche, lácteos y huevos"
    if "despensa" in raw or "alimento" in raw:
        return "Alimentos y bebidas no alcohólicas"
    return "Otros bienes"

# ---------- helper genérico --------------------------------------------------------
async def fetch_json(page, pattern: str):
    """
    Escucha respuestas cuyo URL coincida con `pattern` y devuélvelas como JSON.
    """
    result = {}

    async def _capture(resp):
        if re.search(pattern, resp.url) and "application/json" in resp.headers.get("content-type", ""):
            result["payload"] = await resp.json()

    page.on("response", _capture)
    await page.wait_for_timeout(5000)  # deja cargar peticiones
    return result.get("payload", {})

# ---------- Coto (GraphQL) ----------------------------------------------------------
async def coto_df():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto("https://www.cotodigital3.com.ar/sitios/cdigi/", timeout=60000)
        payload = await fetch_json(page, r"graphql.*getProductsByCategory")
        await browser.close()

    rows = []
    for prod in payload.get("data", {}).get("products", []):
        rows.append({
            "date":     date.today(),
            "store":    "Coto",
            "sku":      prod["sku"],
            "name":     prod["name"],
            "price":    prod["price"],
            "division": map_division(prod["category"]),
            "province": "Nacional"
        })
    return pd.DataFrame(rows)

# ---------- La Anónima -------------------------------------------------------------
def laanonima_df():
    url = (
        "https://supermercado.laanonimaonline.com/api/catalog_system/"
        "pub/products/search?fq=C:1101&_from=0&_to=49"
    )
    data = requests.get(url, headers=HEADERS, timeout=30).json()
    rows = []
    for p in data:
        rows.append({
            "date":     date.today(),
            "store":    "La Anónima",
            "sku":      p["productId"],
            "name":     p["productName"],
            "price":    p["items"][0]["sellers"][0]["commertialOffer"]["Price"],
            "division": map_division(p["categories"][0]),
            "province": "Nacional"
        })
    return pd.DataFrame(rows)

# ---------- Jumbo -------------------------------------------------------------------
async def jumbo_df():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto("https://www.jumbo.com.ar/despensa", timeout=60000)
        nuxt_json = await page.eval_on_selector("script#__NUXT_DATA__", "el => el.innerText")
        await browser.close()

    products = json.loads(nuxt_json)[0]["state"]["products"]
    df = pd.json_normalize(products)
    df["date"]     = date.today()
    df["store"]    = "Jumbo"
    df["division"] = df["mainCategory"].apply(map_division)
    df["province"] = "Nacional"
    return df[["date", "store", "sku", "name", "price", "division", "province"]]

# ---------- Orquestador -------------------------------------------------------------
def scrape_all():
    """
    Ejecuta todos los scrapers y concatena sus DataFrames.
    """
    dfs = []
    loop = asyncio.get_event_loop()
    dfs.append(loop.run_until_complete(coto_df()))
    dfs.append(laanonima_df())
    dfs.append(loop.run_until_complete(jumbo_df()))
    return pd.concat(dfs, ignore_index=True)
# ─────────────────────────────────────────────────────────────────────────
