# ──────────────────── etl/real_data_sources.py ────────────────────────────
"""
Real Data Sources for Argentina Market Intelligence
=================================================

Comprehensive real-time price data collection from multiple Argentine sources:
- CheSuper.ar - Price comparison platform
- PreciosHoy.com.ar - Daily price monitoring
- SeguiPrecios.com.ar - Historical price tracking
- Argentina.gob.ar - Official government price data
- Direct retailer APIs and scrapers
- MercadoLibre API - Real marketplace prices

NO SYNTHETIC DATA - 100% REAL SOURCES ONLY
"""

import asyncio
import json
import re
import time
import random
from datetime import date, datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
import pandas as pd
import requests
from requests.exceptions import RequestException, JSONDecodeError
import logging
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
import nest_asyncio
from bs4 import BeautifulSoup
import urllib.parse

nest_asyncio.apply()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RealDataCollector:
    """
    Comprehensive real data collection from multiple Argentine price sources
    """
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'es-AR,es;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        })
        
        # Essential products to search for across all sources
        self.target_products = [
            "leche", "pan", "aceite", "arroz", "azucar", "fideos", 
            "yogur", "manteca", "queso", "carne", "pollo", "huevos",
            "tomate", "papa", "cebolla", "manzana", "banana", "naranja",
            "detergente", "shampoo", "papel higienico", "coca cola",
            "cerveza", "agua mineral", "galletitas", "dulce de leche"
        ]
        
        # Category mapping for better organization
        self.category_mappings = {
            'leche': 'Leche, lácteos y huevos',
            'yogur': 'Leche, lácteos y huevos', 
            'manteca': 'Leche, lácteos y huevos',
            'queso': 'Leche, lácteos y huevos',
            'huevos': 'Leche, lácteos y huevos',
            'dulce de leche': 'Leche, lácteos y huevos',
            'pan': 'Alimentos y bebidas no alcohólicas',
            'fideos': 'Alimentos y bebidas no alcohólicas',
            'arroz': 'Alimentos y bebidas no alcohólicas',
            'azucar': 'Alimentos y bebidas no alcohólicas',
            'galletitas': 'Alimentos y bebidas no alcohólicas',
            'aceite': 'Aceites y grasas',
            'carne': 'Carnes',
            'pollo': 'Carnes',
            'tomate': 'Frutas y verduras',
            'papa': 'Frutas y verduras',
            'cebolla': 'Frutas y verduras',
            'manzana': 'Frutas y verduras',
            'banana': 'Frutas y verduras',
            'naranja': 'Frutas y verduras',
            'coca cola': 'Bebidas no alcohólicas',
            'agua mineral': 'Bebidas no alcohólicas',
            'cerveza': 'Bebidas alcohólicas',
            'detergente': 'Limpieza del hogar',
            'papel higienico': 'Limpieza del hogar',
            'shampoo': 'Cuidado personal'
        }

    def get_category(self, product_name: str) -> str:
        """Map product to category"""
        product_lower = product_name.lower()
        for keyword, category in self.category_mappings.items():
            if keyword in product_lower:
                return category
        return "Otros bienes"

    def extract_price_from_text(self, text: str) -> Optional[float]:
        """Extract numeric price from text with various formats"""
        if not text:
            return None
        
        # Remove common prefixes and clean text
        text = text.replace('$', '').replace('ARS', '').replace('AR$', '')
        text = re.sub(r'[^\d,.]', '', text)
        
        # Handle different decimal separators
        if ',' in text and '.' in text:
            # Format like 1.234,56
            text = text.replace('.', '').replace(',', '.')
        elif ',' in text:
            # Check if it's thousands separator or decimal
            parts = text.split(',')
            if len(parts) == 2 and len(parts[1]) <= 2:
                # Decimal separator
                text = text.replace(',', '.')
            else:
                # Thousands separator
                text = text.replace(',', '')
        
        try:
            price = float(text)
            return price if price > 0 else None
        except (ValueError, TypeError):
            return None

    async def scrape_chesuper(self) -> pd.DataFrame:
        """
        Scrape CheSuper.ar - Price comparison platform
        """
        logger.info("🔄 Scraping CheSuper.ar for real price data...")
        all_products = []
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            try:
                # Navigate to CheSuper main page
                await page.goto('https://chesuper.ar/', timeout=30000)
                await page.wait_for_timeout(3000)
                
                # Look for product search or category links
                for product in self.target_products[:10]:  # Limit to avoid overloading
                    try:
                        # Try searching for the product
                        search_box = await page.query_selector('input[type="search"], input[placeholder*="buscar"], .search-input')
                        if search_box:
                            await search_box.fill(product)
                            await page.keyboard.press('Enter')
                            await page.wait_for_timeout(2000)
                            
                            # Extract results
                            products = await page.query_selector_all('.product, .item, [data-product]')
                            for prod in products[:5]:
                                try:
                                    name_elem = await prod.query_selector('.name, .title, h3, .product-name')
                                    price_elem = await prod.query_selector('.price, .precio, .valor')
                                    store_elem = await prod.query_selector('.store, .tienda, .supermercado')
                                    
                                    if name_elem and price_elem:
                                        name = await name_elem.inner_text()
                                        price_text = await price_elem.inner_text()
                                        store = await store_elem.inner_text() if store_elem else "CheSuper"
                                        
                                        price = self.extract_price_from_text(price_text)
                                        if price and name:
                                            all_products.append({
                                                'date': date.today(),
                                                'store': store.strip(),
                                                'sku': f"chesuper_{len(all_products)}",
                                                'name': name.strip(),
                                                'price': price,
                                                'division': self.get_category(name),
                                                'province': 'Nacional',
                                                'source': 'CheSuper'
                                            })
                                except Exception as e:
                                    logger.debug(f"Error extracting product from CheSuper: {e}")
                                    continue
                                    
                            # Clear search for next product
                            await search_box.fill('')
                            
                    except Exception as e:
                        logger.debug(f"Error searching for {product} on CheSuper: {e}")
                        continue
                        
            except Exception as e:
                logger.error(f"Error accessing CheSuper: {e}")
            finally:
                await browser.close()
        
        logger.info(f"✅ CheSuper: {len(all_products)} real products collected")
        return pd.DataFrame(all_products)

    async def scrape_precios_hoy(self) -> pd.DataFrame:
        """
        Scrape PreciosHoy.com.ar - Daily price monitoring
        """
        logger.info("🔄 Scraping PreciosHoy.com.ar for real price data...")
        all_products = []
        
        try:
            # Try different sections of PreciosHoy
            urls_to_try = [
                'https://www.precioshoy.com.ar/',
                'https://www.precioshoy.com.ar/ofertas.asp',
                'https://www.precioshoy.com.ar/supermercados.asp',
                'https://www.precioshoy.com.ar/lacteos.asp',
                'https://www.precioshoy.com.ar/bebidas.asp'
            ]
            
            for url in urls_to_try:
                try:
                    response = self.session.get(url, timeout=15)
                    if response.status_code == 200:
                        soup = BeautifulSoup(response.content, 'html.parser')
                        
                        # Look for product listings
                        products = soup.find_all(['div', 'tr', 'li'], class_=re.compile(r'product|item|precio|oferta'))
                        
                        for prod in products[:10]:
                            try:
                                # Extract product information
                                name_elem = prod.find(['h3', 'h4', 'span', 'td'], class_=re.compile(r'name|title|producto'))
                                price_elem = prod.find(['span', 'td', 'div'], class_=re.compile(r'price|precio|valor'))
                                store_elem = prod.find(['span', 'td', 'div'], class_=re.compile(r'store|tienda|super'))
                                
                                if name_elem and price_elem:
                                    name = name_elem.get_text(strip=True)
                                    price_text = price_elem.get_text(strip=True)
                                    store = store_elem.get_text(strip=True) if store_elem else "PreciosHoy"
                                    
                                    price = self.extract_price_from_text(price_text)
                                    if price and name and len(name) > 3:
                                        all_products.append({
                                            'date': date.today(),
                                            'store': store.strip(),
                                            'sku': f"precioshoy_{len(all_products)}",
                                            'name': name.strip(),
                                            'price': price,
                                            'division': self.get_category(name),
                                            'province': 'Nacional',
                                            'source': 'PreciosHoy'
                                        })
                            except Exception as e:
                                logger.debug(f"Error extracting product from PreciosHoy: {e}")
                                continue
                                
                except Exception as e:
                    logger.debug(f"Error accessing {url}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error in PreciosHoy scraper: {e}")
        
        logger.info(f"✅ PreciosHoy: {len(all_products)} real products collected")
        return pd.DataFrame(all_products)

    async def scrape_segui_precios(self) -> pd.DataFrame:
        """
        Scrape SeguiPrecios.com.ar - Historical price tracking
        """
        logger.info("🔄 Scraping SeguiPrecios.com.ar for real price data...")
        all_products = []
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            try:
                await page.goto('https://www.seguiprecios.com.ar/', timeout=30000)
                await page.wait_for_timeout(3000)
                
                # Look for current price listings
                products = await page.query_selector_all('.precio-item, .product, .item-price, [data-price]')
                
                for prod in products[:15]:
                    try:
                        name_elem = await prod.query_selector('.nombre, .name, .producto, h3')
                        price_elem = await prod.query_selector('.precio, .price, .valor')
                        store_elem = await prod.query_selector('.supermercado, .store, .tienda')
                        
                        if name_elem and price_elem:
                            name = await name_elem.inner_text()
                            price_text = await price_elem.inner_text()
                            store = await store_elem.inner_text() if store_elem else "SeguiPrecios"
                            
                            price = self.extract_price_from_text(price_text)
                            if price and name:
                                all_products.append({
                                    'date': date.today(),
                                    'store': store.strip(),
                                    'sku': f"seguiprecios_{len(all_products)}",
                                    'name': name.strip(),
                                    'price': price,
                                    'division': self.get_category(name),
                                    'province': 'Nacional',
                                    'source': 'SeguiPrecios'
                                })
                    except Exception as e:
                        logger.debug(f"Error extracting from SeguiPrecios: {e}")
                        continue
                        
            except Exception as e:
                logger.error(f"Error accessing SeguiPrecios: {e}")
            finally:
                await browser.close()
        
        logger.info(f"✅ SeguiPrecios: {len(all_products)} real products collected")
        return pd.DataFrame(all_products)

    def scrape_government_prices(self) -> pd.DataFrame:
        """
        Scrape Argentina.gob.ar official price data
        """
        logger.info("🔄 Scraping Argentina.gob.ar for official price data...")
        all_products = []
        
        try:
            # Official government price monitoring URLs
            gov_urls = [
                'https://www.argentina.gob.ar/economia/comercio/precios-maximos',
                'https://datos.gob.ar/dataset/comercio-precios-maximos-referencia',
                'https://www.argentina.gob.ar/produccion/comercio-interior/precios'
            ]
            
            for url in gov_urls:
                try:
                    response = self.session.get(url, timeout=20)
                    if response.status_code == 200:
                        soup = BeautifulSoup(response.content, 'html.parser')
                        
                        # Look for price tables or lists
                        tables = soup.find_all('table')
                        for table in tables:
                            rows = table.find_all('tr')[1:]  # Skip header
                            for row in rows[:20]:
                                cells = row.find_all(['td', 'th'])
                                if len(cells) >= 2:
                                    try:
                                        name = cells[0].get_text(strip=True)
                                        price_text = cells[1].get_text(strip=True)
                                        
                                        price = self.extract_price_from_text(price_text)
                                        if price and name and len(name) > 3:
                                            all_products.append({
                                                'date': date.today(),
                                                'store': 'Gobierno Argentina',
                                                'sku': f"gov_ar_{len(all_products)}",
                                                'name': name.strip(),
                                                'price': price,
                                                'division': self.get_category(name),
                                                'province': 'Nacional',
                                                'source': 'Argentina.gob.ar'
                                            })
                                    except Exception as e:
                                        logger.debug(f"Error processing government price row: {e}")
                                        continue
                                        
                except Exception as e:
                    logger.debug(f"Error accessing government URL {url}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error in government prices scraper: {e}")
        
        logger.info(f"✅ Government data: {len(all_products)} real products collected")
        return pd.DataFrame(all_products)

    async def scrape_mercadolibre_api(self) -> pd.DataFrame:
        """
        Use MercadoLibre API for real marketplace prices
        """
        logger.info("🔄 Accessing MercadoLibre API for real marketplace data...")
        all_products = []
        
        try:
            base_url = "https://api.mercadolibre.com/sites/MLA/search"
            
            for product in self.target_products[:8]:  # Limit API calls
                try:
                    params = {
                        'q': product,
                        'limit': 10,
                        'condition': 'new',
                        'sort': 'relevance',
                        'category': 'MLA1574'  # Hogar y Jardín category
                    }
                    
                    response = self.session.get(base_url, params=params, timeout=10)
                    if response.status_code == 200:
                        data = response.json()
                        
                        for item in data.get('results', []):
                            try:
                                name = item.get('title', '')
                                price = item.get('price')
                                currency = item.get('currency_id', 'ARS')
                                seller = item.get('seller', {}).get('nickname', 'MercadoLibre')
                                
                                if price and name and currency == 'ARS':
                                    all_products.append({
                                        'date': date.today(),
                                        'store': f"ML_{seller}",
                                        'sku': item.get('id', f"ml_{len(all_products)}"),
                                        'name': name.strip(),
                                        'price': float(price),
                                        'division': self.get_category(name),
                                        'province': 'Nacional',
                                        'source': 'MercadoLibre_API'
                                    })
                            except Exception as e:
                                logger.debug(f"Error processing ML item: {e}")
                                continue
                                
                    await asyncio.sleep(0.5)  # Rate limiting
                    
                except Exception as e:
                    logger.debug(f"Error searching ML for {product}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error in MercadoLibre API: {e}")
        
        logger.info(f"✅ MercadoLibre API: {len(all_products)} real products collected")
        return pd.DataFrame(all_products)

    async def scrape_enhanced_retailers(self) -> pd.DataFrame:
        """
        Enhanced direct retailer scraping with multiple strategies
        """
        logger.info("🔄 Enhanced retailer scraping for real data...")
        all_products = []
        
        # Enhanced retailer configurations
        retailers = [
            {
                'name': 'Coto',
                'urls': [
                    'https://www.cotodigital3.com.ar/sitios/cdigi/',
                    'https://www.cotodigital3.com.ar/sitios/cdigi/productos'
                ],
                'api_endpoints': [
                    'https://www.cotodigital3.com.ar/api/productos/mas-vendidos',
                    'https://www.cotodigital3.com.ar/sitios/cdigi/productos/buscar'
                ]
            },
            {
                'name': 'Jumbo',
                'urls': [
                    'https://www.jumbo.com.ar/',
                    'https://www.jumbo.com.ar/despensa'
                ],
                'api_endpoints': [
                    'https://www.jumbo.com.ar/api/catalog_system/pub/products/search'
                ]
            },
            {
                'name': 'Carrefour',
                'urls': [
                    'https://www.carrefour.com.ar/',
                    'https://www.carrefour.com.ar/almacen'
                ],
                'api_endpoints': []
            },
            {
                'name': 'Día',
                'urls': [
                    'https://diaonline.supermercadosdia.com.ar/',
                    'https://diaonline.supermercadosdia.com.ar/almacen'
                ],
                'api_endpoints': []
            }
        ]
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            
            for retailer in retailers:
                try:
                    page = await browser.new_page()
                    
                    for url in retailer['urls']:
                        try:
                            await page.goto(url, timeout=20000)
                            await page.wait_for_timeout(3000)
                            
                            # Generic product selectors
                            products = await page.query_selector_all([
                                '.product', '.item', '.producto', 
                                '[data-product]', '.card-product',
                                '.product-item', '.item-product'
                            ])
                            
                            for prod in products[:8]:
                                try:
                                    name_elem = await prod.query_selector([
                                        '.name', '.title', '.product-name',
                                        'h3', 'h4', '.titulo'
                                    ])
                                    price_elem = await prod.query_selector([
                                        '.price', '.precio', '.valor',
                                        '.current-price', '.price-current'
                                    ])
                                    
                                    if name_elem and price_elem:
                                        name = await name_elem.inner_text()
                                        price_text = await price_elem.inner_text()
                                        
                                        price = self.extract_price_from_text(price_text)
                                        if price and name and len(name) > 3:
                                            all_products.append({
                                                'date': date.today(),
                                                'store': retailer['name'],
                                                'sku': f"{retailer['name'].lower()}_{len(all_products)}",
                                                'name': name.strip(),
                                                'price': price,
                                                'division': self.get_category(name),
                                                'province': 'Buenos Aires',
                                                'source': f"{retailer['name']}_Direct"
                                            })
                                except Exception as e:
                                    logger.debug(f"Error extracting from {retailer['name']}: {e}")
                                    continue
                                    
                        except Exception as e:
                            logger.debug(f"Error accessing {url}: {e}")
                            continue
                            
                    await page.close()
                    
                except Exception as e:
                    logger.error(f"Error scraping {retailer['name']}: {e}")
                    continue
                    
            await browser.close()
        
        logger.info(f"✅ Enhanced retailers: {len(all_products)} real products collected")
        return pd.DataFrame(all_products)

    async def collect_all_real_data(self) -> pd.DataFrame:
        """
        Collect data from ALL real sources concurrently
        """
        logger.info("🚀 Starting comprehensive REAL data collection from multiple sources...")
        
        # Run all scrapers concurrently for maximum efficiency
        tasks = [
            self.scrape_chesuper(),
            self.scrape_precios_hoy(),
            self.scrape_segui_precios(),
            asyncio.create_task(asyncio.to_thread(self.scrape_government_prices)),
            self.scrape_mercadolibre_api(),
            self.scrape_enhanced_retailers()
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        all_dataframes = []
        total_products = 0
        
        for i, result in enumerate(results):
            source_names = ["CheSuper", "PreciosHoy", "SeguiPrecios", "Government", "MercadoLibre", "Enhanced Retailers"]
            
            if isinstance(result, pd.DataFrame) and not result.empty:
                all_dataframes.append(result)
                total_products += len(result)
                logger.info(f"✅ {source_names[i]}: {len(result)} real products")
            elif isinstance(result, Exception):
                logger.warning(f"❌ {source_names[i]} failed: {result}")
            else:
                logger.warning(f"⚠️ {source_names[i]}: No data collected")
        
        if all_dataframes:
            combined_df = pd.concat(all_dataframes, ignore_index=True)
            
            # Add required columns for database compatibility
            required_columns = [
                'date', 'store', 'sku', 'name', 'price', 'division', 'province',
                'source', 'price_sources', 'num_sources', 'price_min', 'price_max', 
                'price_std', 'reliability_weight'
            ]
            
            for col in required_columns:
                if col not in combined_df.columns:
                    if col == 'price_sources':
                        combined_df[col] = combined_df['source']
                    elif col == 'num_sources':
                        combined_df[col] = 1
                    elif col == 'price_min':
                        combined_df[col] = combined_df['price']
                    elif col == 'price_max':
                        combined_df[col] = combined_df['price']
                    elif col == 'price_std':
                        combined_df[col] = 0.0
                    elif col == 'reliability_weight':
                        combined_df[col] = 1.0
                    else:
                        combined_df[col] = None
            
            # Reorder columns to match database schema
            combined_df = combined_df[required_columns]
            
            logger.info(f"🎯 TOTAL REAL DATA COLLECTED: {total_products} products from {len(all_dataframes)} sources")
            return combined_df
        else:
            logger.error("❌ NO REAL DATA collected from any source")
            raise Exception("Failed to collect any real data from online sources")

# Main function for external use
async def collect_real_data_only() -> pd.DataFrame:
    """
    Main function to collect ONLY real data - NO SYNTHETIC DATA
    """
    collector = RealDataCollector()
    return await collector.collect_all_real_data()

def collect_real_data_sync() -> pd.DataFrame:
    """
    Synchronous wrapper for real data collection
    """
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(collect_real_data_only())

# ─────────────────────────────────────────────────────────────────────────
