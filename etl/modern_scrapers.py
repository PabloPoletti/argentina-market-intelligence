# ──────────────────── etl/modern_scrapers.py ────────────────────────────
"""
Modern Web Scrapers for Argentina Market Intelligence
===================================================

Updated scrapers with improved resilience, better error handling,
and alternative data sources for real price collection.

Features:
- Multiple fallback strategies per retailer
- Robust error handling and logging
- Alternative data sources
- Real-time price collection
- Anti-detection measures
"""

import asyncio
import json
import re
import time
import random
from datetime import date, datetime
from typing import List, Dict, Any, Optional
import pandas as pd
import requests
from requests.exceptions import RequestException, JSONDecodeError
import logging
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
import nest_asyncio

nest_asyncio.apply()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ModernScraperEngine:
    """
    Modern scraping engine with enhanced capabilities and fallback mechanisms
    """
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'es-AR,es;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        
        # Product mappings for better categorization
        self.category_mappings = {
            'lácteos': 'Leche, lácteos y huevos',
            'leche': 'Leche, lácteos y huevos',
            'yogur': 'Leche, lácteos y huevos',
            'queso': 'Leche, lácteos y huevos',
            'manteca': 'Leche, lácteos y huevos',
            'pan': 'Alimentos y bebidas no alcohólicas',
            'fideos': 'Alimentos y bebidas no alcohólicas',
            'arroz': 'Alimentos y bebidas no alcohólicas',
            'aceite': 'Aceites y grasas',
            'azúcar': 'Alimentos y bebidas no alcohólicas',
            'bebidas': 'Alimentos y bebidas no alcohólicas',
            'limpieza': 'Otros bienes',
            'higiene': 'Otros bienes'
        }

    def categorize_product(self, name: str, category: str = "") -> str:
        """Smart product categorization"""
        text = f"{name} {category}".lower()
        
        for keyword, mapped_category in self.category_mappings.items():
            if keyword in text:
                return mapped_category
        
        return "Otros bienes"

    async def scrape_coto_modern(self) -> pd.DataFrame:
        """
        Modern Coto scraper with multiple strategies
        """
        strategies = [
            self._coto_api_v1,
            self._coto_api_v2,
            self._coto_playwright
        ]
        
        for strategy in strategies:
            try:
                result = await strategy()
                if not result.empty:
                    logger.info(f"Coto: Strategy {strategy.__name__} successful - {len(result)} products")
                    return result
            except Exception as e:
                logger.warning(f"Coto strategy {strategy.__name__} failed: {e}")
                continue
        
        logger.error("All Coto strategies failed")
        return pd.DataFrame()

    async def _coto_api_v1(self) -> pd.DataFrame:
        """Coto API Strategy 1: Main catalog"""
        # Try different API endpoints for Coto
        endpoints = [
            "https://www.cotodigital3.com.ar/sitios/cdigi/productos/buscar?Nrpp=30&No=0",
            "https://www.cotodigital3.com.ar/sitios/cdigi/productos-mas-vendidos",
            "https://www.cotodigital3.com.ar/api/productos/ofertas"
        ]
        
        for endpoint in endpoints:
            try:
                response = self.session.get(endpoint, timeout=15)
                if response.status_code == 200:
                    # Try to parse different response formats
                    if 'application/json' in response.headers.get('content-type', ''):
                        data = response.json()
                        products = self._extract_coto_products(data)
                        if products:
                            return self._format_coto_data(products)
            except Exception as e:
                logger.debug(f"Coto API v1 endpoint {endpoint} failed: {e}")
                continue
        
        raise Exception("No valid Coto API endpoints found")

    async def _coto_api_v2(self) -> pd.DataFrame:
        """Coto API Strategy 2: GraphQL"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            # Intercept GraphQL requests
            graphql_data = {}
            
            async def handle_response(response):
                if 'graphql' in response.url and response.status == 200:
                    try:
                        data = await response.json()
                        if 'data' in data and 'products' in data.get('data', {}):
                            graphql_data['products'] = data['data']['products']
                    except:
                        pass
            
            page.on('response', handle_response)
            
            try:
                await page.goto('https://www.cotodigital3.com.ar/sitios/cdigi/', timeout=30000)
                await page.wait_for_timeout(5000)
                
                if 'products' in graphql_data:
                    return self._format_coto_data(graphql_data['products'])
                    
            finally:
                await browser.close()
        
        raise Exception("Coto GraphQL strategy failed")

    async def _coto_playwright(self) -> pd.DataFrame:
        """Coto Strategy 3: Direct scraping"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            try:
                await page.goto('https://www.cotodigital3.com.ar/sitios/cdigi/productos', timeout=30000)
                await page.wait_for_timeout(3000)
                
                # Look for product containers
                products = await page.query_selector_all('.product-item, .producto, [data-product]')
                
                results = []
                for product in products[:20]:  # Limit to first 20 products
                    try:
                        name = await product.query_selector('.product-name, .titulo, h3, .name')
                        price = await product.query_selector('.price, .precio, .valor')
                        
                        if name and price:
                            name_text = await name.inner_text()
                            price_text = await price.inner_text()
                            
                            # Extract numeric price
                            price_num = self._extract_price(price_text)
                            if price_num:
                                results.append({
                                    'name': name_text.strip(),
                                    'price': price_num,
                                    'store': 'Coto'
                                })
                    except:
                        continue
                
                return self._format_scraped_data(results)
                
            finally:
                await browser.close()

    def scrape_laanonima_modern(self) -> pd.DataFrame:
        """
        Modern La Anónima scraper with multiple endpoints
        """
        strategies = [
            self._laanonima_api_v1,
            self._laanonima_api_v2,
            self._laanonima_search
        ]
        
        for strategy in strategies:
            try:
                result = strategy()
                if not result.empty:
                    logger.info(f"La Anónima: Strategy {strategy.__name__} successful - {len(result)} products")
                    return result
            except Exception as e:
                logger.warning(f"La Anónima strategy {strategy.__name__} failed: {e}")
                continue
        
        logger.error("All La Anónima strategies failed")
        return pd.DataFrame()

    def _laanonima_api_v1(self) -> pd.DataFrame:
        """La Anónima API Strategy 1: Product search"""
        endpoints = [
            "https://supermercado.laanonimaonline.com/api/catalog_system/pub/products/search?fq=C:1101&_from=0&_to=30",
            "https://supermercado.laanonimaonline.com/api/catalog_system/pub/products/search?fq=B:1&_from=0&_to=30",
            "https://www.laanonimaonline.com/api/products/search?category=alimentos",
            "https://www.laanonimaonline.com/api/v1/products/featured"
        ]
        
        for endpoint in endpoints:
            try:
                response = self.session.get(endpoint, timeout=15)
                if response.status_code == 200:
                    try:
                        data = response.json()
                        if isinstance(data, list) and len(data) > 0:
                            return self._format_laanonima_data(data)
                    except JSONDecodeError:
                        continue
            except Exception as e:
                logger.debug(f"La Anónima API v1 endpoint {endpoint} failed: {e}")
                continue
        
        raise Exception("No valid La Anónima API endpoints found")

    def _laanonima_api_v2(self) -> pd.DataFrame:
        """La Anónima API Strategy 2: Alternative format"""
        # Try with different parameters and formats
        params_list = [
            {"q": "", "map": "ft", "sc": "1"},
            {"category": "1", "limit": "30"},
            {"search": "ofertas", "limit": "20"}
        ]
        
        base_url = "https://supermercado.laanonimaonline.com/api/catalog_system/pub/products/search"
        
        for params in params_list:
            try:
                response = self.session.get(base_url, params=params, timeout=15)
                if response.status_code == 200:
                    data = response.json()
                    if data and len(data) > 0:
                        return self._format_laanonima_data(data)
            except Exception as e:
                logger.debug(f"La Anónima API v2 with params {params} failed: {e}")
                continue
        
        raise Exception("La Anónima API v2 strategies failed")

    def _laanonima_search(self) -> pd.DataFrame:
        """La Anónima Strategy 3: Search specific products"""
        # Search for specific high-volume products
        search_terms = ["leche", "pan", "aceite", "arroz", "azucar"]
        all_products = []
        
        base_url = "https://supermercado.laanonimaonline.com/api/catalog_system/pub/products/search"
        
        for term in search_terms:
            try:
                params = {"ft": term, "sc": "1"}
                response = self.session.get(base_url, params=params, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    if data:
                        all_products.extend(data[:5])  # Take first 5 results per term
            except:
                continue
        
        if all_products:
            return self._format_laanonima_data(all_products)
        
        raise Exception("La Anónima search strategies failed")

    async def scrape_jumbo_modern(self) -> pd.DataFrame:
        """
        Modern Jumbo scraper with enhanced detection
        """
        strategies = [
            self._jumbo_nuxt_data,
            self._jumbo_api_search,
            self._jumbo_playwright_scrape
        ]
        
        for strategy in strategies:
            try:
                result = await strategy()
                if not result.empty:
                    logger.info(f"Jumbo: Strategy {strategy.__name__} successful - {len(result)} products")
                    return result
            except Exception as e:
                logger.warning(f"Jumbo strategy {strategy.__name__} failed: {e}")
                continue
        
        logger.error("All Jumbo strategies failed")
        return pd.DataFrame()

    async def _jumbo_nuxt_data(self) -> pd.DataFrame:
        """Jumbo Strategy 1: NUXT data extraction"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            try:
                await page.goto('https://www.jumbo.com.ar/', timeout=30000)
                await page.wait_for_timeout(3000)
                
                # Try multiple NUXT data selectors
                selectors = [
                    'script#__NUXT_DATA__',
                    'script[type="application/json"]',
                    'script:has-text("__NUXT__")'
                ]
                
                for selector in selectors:
                    element = await page.query_selector(selector)
                    if element:
                        content = await element.inner_text()
                        try:
                            data = json.loads(content)
                            products = self._extract_jumbo_products(data)
                            if products:
                                return self._format_jumbo_data(products)
                        except:
                            continue
                
            finally:
                await browser.close()
        
        raise Exception("Jumbo NUXT data extraction failed")

    async def _jumbo_api_search(self) -> pd.DataFrame:
        """Jumbo Strategy 2: API search"""
        # Try different Jumbo API endpoints
        api_endpoints = [
            "https://www.jumbo.com.ar/api/catalog_system/pub/products/search",
            "https://www.jumbo.com.ar/api/products/search",
            "https://api.jumbo.com.ar/products/featured"
        ]
        
        for endpoint in api_endpoints:
            try:
                response = self.session.get(endpoint, timeout=15)
                if response.status_code == 200:
                    data = response.json()
                    products = self._extract_jumbo_products(data)
                    if products:
                        return self._format_jumbo_data(products)
            except Exception as e:
                logger.debug(f"Jumbo API {endpoint} failed: {e}")
                continue
        
        raise Exception("Jumbo API search failed")

    async def _jumbo_playwright_scrape(self) -> pd.DataFrame:
        """Jumbo Strategy 3: Direct scraping"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            try:
                await page.goto('https://www.jumbo.com.ar/despensa', timeout=30000)
                await page.wait_for_timeout(5000)
                
                # Look for product elements
                products = await page.query_selector_all('.product, .item-product, [data-testid*="product"]')
                
                results = []
                for product in products[:15]:
                    try:
                        name_elem = await product.query_selector('.product-name, .title, h3, .name')
                        price_elem = await product.query_selector('.price, .precio, .valor')
                        
                        if name_elem and price_elem:
                            name = await name_elem.inner_text()
                            price_text = await price_elem.inner_text()
                            price = self._extract_price(price_text)
                            
                            if price:
                                results.append({
                                    'name': name.strip(),
                                    'price': price,
                                    'store': 'Jumbo'
                                })
                    except:
                        continue
                
                return self._format_scraped_data(results)
                
            finally:
                await browser.close()

    # Utility methods for data processing
    def _extract_price(self, price_text: str) -> Optional[float]:
        """Extract numeric price from text"""
        if not price_text:
            return None
        
        # Remove common currency symbols and text
        clean_text = re.sub(r'[^\d,.]', '', price_text.replace('$', '').replace(',', '.'))
        
        try:
            return float(clean_text)
        except:
            return None

    def _extract_coto_products(self, data: dict) -> List[dict]:
        """Extract products from Coto API response"""
        products = []
        
        # Try different data structures
        possible_paths = [
            ['data', 'products'],
            ['products'],
            ['items'],
            ['data', 'items']
        ]
        
        for path in possible_paths:
            try:
                current = data
                for key in path:
                    current = current[key]
                if isinstance(current, list):
                    products = current
                    break
            except (KeyError, TypeError):
                continue
        
        return products

    def _extract_jumbo_products(self, data: dict) -> List[dict]:
        """Extract products from Jumbo data response"""
        products = []
        
        # Try different Jumbo data structures
        if isinstance(data, list) and len(data) > 0:
            if isinstance(data[0], dict) and 'state' in data[0]:
                products = data[0].get('state', {}).get('products', [])
            else:
                products = data
        elif isinstance(data, dict):
            products = data.get('products', data.get('items', []))
        
        return products

    def _format_coto_data(self, products: List[dict]) -> pd.DataFrame:
        """Format Coto products into standard DataFrame"""
        rows = []
        for prod in products:
            try:
                name = prod.get('name', prod.get('productName', ''))
                price = prod.get('price', prod.get('currentPrice', 0))
                category = prod.get('category', prod.get('categoryName', ''))
                
                if name and price:
                    rows.append({
                        'date': date.today(),
                        'store': 'Coto',
                        'sku': prod.get('sku', prod.get('id', f"coto_{len(rows)}")),
                        'name': name,
                        'price': float(price) if isinstance(price, (int, float, str)) else 0,
                        'division': self.categorize_product(name, category),
                        'province': 'Buenos Aires'
                    })
            except:
                continue
        
        return pd.DataFrame(rows)

    def _format_laanonima_data(self, products: List[dict]) -> pd.DataFrame:
        """Format La Anónima products into standard DataFrame"""
        rows = []
        for prod in products:
            try:
                name = prod.get('productName', '')
                
                # Extract price from complex structure
                price = 0
                items = prod.get('items', [])
                if items:
                    sellers = items[0].get('sellers', [])
                    if sellers:
                        offer = sellers[0].get('commertialOffer', {})
                        price = offer.get('Price', 0)
                
                category = ''
                categories = prod.get('categories', [])
                if categories:
                    category = categories[0]
                
                if name and price:
                    rows.append({
                        'date': date.today(),
                        'store': 'La Anónima',
                        'sku': prod.get('productId', f"laanonima_{len(rows)}"),
                        'name': name,
                        'price': float(price),
                        'division': self.categorize_product(name, category),
                        'province': 'Patagonia'
                    })
            except:
                continue
        
        return pd.DataFrame(rows)

    def _format_jumbo_data(self, products: List[dict]) -> pd.DataFrame:
        """Format Jumbo products into standard DataFrame"""
        rows = []
        for prod in products:
            try:
                name = prod.get('name', prod.get('title', ''))
                price = prod.get('price', prod.get('currentPrice', 0))
                category = prod.get('mainCategory', prod.get('category', ''))
                
                if name and price:
                    rows.append({
                        'date': date.today(),
                        'store': 'Jumbo',
                        'sku': prod.get('sku', prod.get('id', f"jumbo_{len(rows)}")),
                        'name': name,
                        'price': float(price),
                        'division': self.categorize_product(name, category),
                        'province': 'Buenos Aires'
                    })
            except:
                continue
        
        return pd.DataFrame(rows)

    def _format_scraped_data(self, products: List[dict]) -> pd.DataFrame:
        """Format scraped data into standard DataFrame"""
        rows = []
        for i, prod in enumerate(products):
            try:
                rows.append({
                    'date': date.today(),
                    'store': prod.get('store', 'Unknown'),
                    'sku': f"{prod.get('store', 'unk').lower()}_{i}",
                    'name': prod['name'],
                    'price': prod['price'],
                    'division': self.categorize_product(prod['name']),
                    'province': 'Buenos Aires'
                })
            except:
                continue
        
        return pd.DataFrame(rows)

# Main scraping functions for backward compatibility
async def modern_scrape_all() -> pd.DataFrame:
    """
    Main function to scrape all retailers using modern methods
    """
    scraper = ModernScraperEngine()
    all_dfs = []
    
    # Scrape all retailers concurrently
    tasks = [
        scraper.scrape_coto_modern(),
        asyncio.create_task(asyncio.to_thread(scraper.scrape_laanonima_modern)),
        scraper.scrape_jumbo_modern()
    ]
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    for i, result in enumerate(results):
        if isinstance(result, pd.DataFrame) and not result.empty:
            all_dfs.append(result)
            logger.info(f"Retailer {i+1}: {len(result)} products collected")
        elif isinstance(result, Exception):
            logger.error(f"Retailer {i+1} failed: {result}")
    
    if all_dfs:
        combined_df = pd.concat(all_dfs, ignore_index=True)
        logger.info(f"Total products collected: {len(combined_df)}")
        return combined_df
    else:
        logger.warning("No real data collected from any retailer")
        return pd.DataFrame()

# Convenience function for synchronous usage
def scrape_all_modern():
    """Synchronous wrapper for the modern scraper"""
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(modern_scrape_all())

# ─────────────────────────────────────────────────────────────────────────
