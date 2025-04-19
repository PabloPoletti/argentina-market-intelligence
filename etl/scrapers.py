import json, re, asyncio, pandas as pd
from datetime import date
from playwright.async_api import async_playwright

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; price‑index‑bot/1.0)"}

# ---------- helpers ----------------------------------------------------------

async def fetch_json(page, url, pattern):
    """Intercepta la respuesta JSON que contiene precios"""
    result = {}
    async def handle(route, request):
        await route.continue_()
    page.on("response", lambda r: _capture(r, pattern, result))
    await page.route("**/*", handle)
    await page.goto(url, timeout=60000)
    return result["payload"]

def _capture(resp, pattern, result):
    if re.search(pattern, resp.url) and "application/json" in resp.headers.get("content-type", ""):
        result["payload"] = asyncio.run(resp.json())

# ---------- COTO (GraphQL) ---------------------------------------------------

async def coto_df():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        # el endpoint GraphQL se llama 'getProductsByCategory'
        payload = await fetch_json(page,
                                   "https://www.cotodigital3.com.ar/sitios/cdigi/",
                                   r"graphql.*getProductsByCategory")
        await browser.close()

    rows = []
    for p in payload["data"]["products"]:
        rows.append({
            "date": date.today(),
            "store": "Coto",
            "sku": p["sku"],
            "name": p["name"],
            "price": p["price"],
            "division": map_division(p["category"]),
            "province": "Nacional"          # Coto no publica sucursal vía web
        })
    return pd.DataFrame(rows)

# ---------- La Anónima (API REST) -------------------------------------------

def laanonima_df():
    url = "https://supermercado.laanonimaonline.com/api/catalog_system/pub/products/search?fq=C:1101&_from=0&_to=49"
    data = requests.get(url, headers=HEADERS, timeout=30).json()
    rows = [{
        "date": date.today(),
        "store": "La Anónima",
        "sku": p["productId"],
        "name": p["productName"],
        "price": p["items"][0]["sellers"][0]["commertialOffer"]["Price"],
        "division": map_division(p["categories"][0]),
        "province": p.get("store", "Nacional")
    } for p in data]
    return pd.DataFrame(rows)

# ---------- Jumbo (Vue SSR) --------------------------------------------------

async def jumbo_df():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto("https://www.jumbo.com.ar/despensa", timeout=60000)
        products = await page.eval_on_selector_all("script#__NUXT_DATA__", "elements => elements.map(e => e.innerText)")
        # el JSON está embebido en el script Nuxt
        parsed = json.loads(products[0])[0]["state"]["products"]
        await browser.close()

    df = pd.json_normalize(parsed)
    df["date"] = date.today()
    df["store"] = "Jumbo"
    df["division"] = df["mainCategory"].apply(map_division)
    df["province"] = "Nacional"
    return df[["date", "store", "sku", "name", "price", "division", "province"]]

# ---------- MercadoLibre histórico (MercadoTrack) ---------------------------

def ml_track_df(ml_id: str):
    url = f"https://beta.mercadotrack.com/MLA/trackings/{ml_id}"
    soup = BeautifulSoup(requests.get(url, headers=HEADERS, timeout=30).text, "lxml")
    history = json.loads(soup.select_one("script#__NEXT_DATA__").string)
    series = history["props"]["pageProps"]["tracking"]["priceHistory"]
    return pd.DataFrame({
        "date": pd.to_datetime([x["at"] for x in series]),
        "store": "MercadoLibre",
        "sku": ml_id,
        "name": history["props"]["pageProps"]["tracking"]["title"],
        "price": [x["priceAmount"] for x in series],
        "division": "Otros bienes",
        "province": "Nacional"
    })

# ---------- orquestador -----------------------------------------------------

def scrape_all():
    dfs = []
    dfs.append(asyncio.run(coto_df()))
    dfs.append(laanonima_df())
    dfs.append(asyncio.run(jumbo_df()))
    # ← agrega más scrapers siguiendo el patrón
    return pd.concat(dfs, ignore_index=True)
