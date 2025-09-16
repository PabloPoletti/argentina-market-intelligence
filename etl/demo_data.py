# ──────────────────── etl/demo_data.py ────────────────────────────
"""
Demo Data Generator for Argentina Market Intelligence
==================================================

This module generates realistic synthetic price data for demonstration
purposes when real scrapers fail or for development/testing.

Features:
- Realistic price ranges for common products
- Seasonal and trend variations
- Multiple store sources
- Regional price differences
- Smart data distribution patterns
"""

import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
import random
from typing import List, Dict, Any

class DemoDataGenerator:
    """
    Generates realistic synthetic price data for the Argentina Market Intelligence dashboard
    """
    
    def __init__(self):
        self.base_products = {
            # Lácteos
            "Leche Entera La Serenísima 1L": {"base_price": 350, "category": "Leche, lácteos y huevos", "variation": 0.15},
            "Yogur Natural Sancor 1L": {"base_price": 280, "category": "Leche, lácteos y huevos", "variation": 0.12},
            "Queso Cremoso Mendicrim 300g": {"base_price": 450, "category": "Leche, lácteos y huevos", "variation": 0.18},
            "Manteca La Serenísima 200g": {"base_price": 220, "category": "Leche, lácteos y huevos", "variation": 0.10},
            
            # Panadería y cereales
            "Pan Francés x unidad": {"base_price": 180, "category": "Alimentos y bebidas no alcohólicas", "variation": 0.08},
            "Fideos Matarazzo 500g": {"base_price": 190, "category": "Alimentos y bebidas no alcohólicas", "variation": 0.12},
            "Arroz Gallo Oro 1kg": {"base_price": 320, "category": "Alimentos y bebidas no alcohólicas", "variation": 0.15},
            "Harina 0000 Blancaflor 1kg": {"base_price": 250, "category": "Alimentos y bebidas no alcohólicas", "variation": 0.10},
            
            # Aceites y grasas
            "Aceite Girasol Natura 900ml": {"base_price": 380, "category": "Aceites y grasas", "variation": 0.20},
            "Aceite Oliva Nucete 500ml": {"base_price": 650, "category": "Aceites y grasas", "variation": 0.25},
            
            # Azúcar y dulces
            "Azúcar Ledesma 1kg": {"base_price": 290, "category": "Alimentos y bebidas no alcohólicas", "variation": 0.12},
            "Dulce de Leche Sancor 400g": {"base_price": 420, "category": "Alimentos y bebidas no alcohólicas", "variation": 0.15},
            
            # Carnes y proteínas
            "Milanesas de Pollo x kg": {"base_price": 850, "category": "Carnes", "variation": 0.22},
            "Carne Picada x kg": {"base_price": 1200, "category": "Carnes", "variation": 0.25},
            
            # Bebidas
            "Coca Cola 2.25L": {"base_price": 450, "category": "Alimentos y bebidas no alcohólicas", "variation": 0.10},
            "Agua Mineral Villavicencio 2L": {"base_price": 180, "category": "Alimentos y bebidas no alcohólicas", "variation": 0.08},
            
            # Productos de limpieza
            "Detergente Ala 750ml": {"base_price": 320, "category": "Otros bienes", "variation": 0.12},
            "Papel Higiénico Higienol x4": {"base_price": 280, "category": "Otros bienes", "variation": 0.10},
        }
        
        self.stores = {
            "Coto": {"markup": 1.0, "region_presence": ["CABA", "Buenos Aires", "Córdoba"]},
            "La Anónima": {"markup": 0.95, "region_presence": ["Patagonia", "Buenos Aires"]},
            "Jumbo": {"markup": 1.08, "region_presence": ["CABA", "Buenos Aires", "Córdoba", "Santa Fe"]},
            "Día": {"markup": 0.92, "region_presence": ["CABA", "Buenos Aires", "Córdoba"]},
            "Carrefour": {"markup": 1.05, "region_presence": ["CABA", "Buenos Aires", "Mendoza"]},
        }
        
        self.regions = {
            "CABA": {"cost_multiplier": 1.15, "province": "CABA"},
            "Buenos Aires": {"cost_multiplier": 1.0, "province": "Buenos Aires"},
            "Córdoba": {"cost_multiplier": 0.98, "province": "Córdoba"},
            "Santa Fe": {"cost_multiplier": 1.02, "province": "Santa Fe"},
            "Mendoza": {"cost_multiplier": 1.05, "province": "Mendoza"},
            "Patagonia": {"cost_multiplier": 1.25, "province": "Río Negro"},
        }

    def generate_realistic_price(self, base_price: float, variation: float, 
                               store_markup: float, region_multiplier: float) -> float:
        """
        Generate a realistic price with controlled randomness
        """
        # Base variation (market fluctuation)
        price_variation = np.random.normal(1.0, variation)
        
        # Apply store markup and regional adjustment
        final_price = base_price * price_variation * store_markup * region_multiplier
        
        # Round to realistic price points (avoid .33, .67 endings, prefer .00, .50, .99)
        rounded_price = round(final_price, 2)
        
        # Apply realistic pricing psychology
        if random.random() < 0.4:  # 40% chance of .99 pricing
            rounded_price = int(rounded_price) + 0.99
        elif random.random() < 0.3:  # 30% chance of .50 pricing
            rounded_price = int(rounded_price) + 0.50
        else:  # 30% chance of round numbers
            rounded_price = round(rounded_price)
        
        return max(10.0, rounded_price)  # Ensure minimum price

    def generate_demo_data(self, num_days: int = 7, 
                          include_trends: bool = True) -> pd.DataFrame:
        """
        Generate comprehensive demo dataset
        """
        data_rows = []
        
        # Generate data for the last num_days
        start_date = date.today() - timedelta(days=num_days-1)
        
        for day_offset in range(num_days):
            current_date = start_date + timedelta(days=day_offset)
            
            # Trend factor (slight inflation over time)
            trend_factor = 1.0 + (day_offset * 0.002) if include_trends else 1.0
            
            # Generate data for each product-store-region combination
            for product_name, product_info in self.base_products.items():
                for store_name, store_info in self.stores.items():
                    # Only generate for regions where store is present
                    available_regions = [r for r in self.regions.keys() 
                                       if r in store_info["region_presence"]]
                    
                    # Select 1-2 random regions for this store
                    selected_regions = random.sample(available_regions, 
                                                    min(len(available_regions), 
                                                        random.randint(1, 2)))
                    
                    for region in selected_regions:
                        region_info = self.regions[region]
                        
                        # Skip some combinations randomly for realism
                        if random.random() < 0.15:  # 15% chance to skip
                            continue
                        
                        # Calculate final price
                        adjusted_base = product_info["base_price"] * trend_factor
                        final_price = self.generate_realistic_price(
                            adjusted_base,
                            product_info["variation"],
                            store_info["markup"],
                            region_info["cost_multiplier"]
                        )
                        
                        # Generate SKU
                        sku = f"{store_name[:3].upper()}-{abs(hash(product_name)) % 10000}"
                        
                        data_rows.append({
                            "date": current_date,
                            "store": store_name,
                            "sku": sku,
                            "name": product_name,
                            "price": final_price,
                            "division": product_info["category"],
                            "province": region_info["province"],
                            "source": "demo_data",
                            "price_sources": store_name,
                            "num_sources": 1,
                            "price_min": final_price,
                            "price_max": final_price,
                            "price_std": 0.0,
                            "reliability_weight": 1.0
                        })
        
        # Create DataFrame and add some aggregated products
        df = pd.DataFrame(data_rows)
        
        # Add some consensus data (products with multiple sources)
        consensus_data = self._generate_consensus_data(df)
        if not consensus_data.empty:
            df = pd.concat([df, consensus_data], ignore_index=True)
        
        return df.sort_values(["date", "name", "store"]).reset_index(drop=True)

    def _generate_consensus_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate consensus pricing data for products with multiple sources
        """
        consensus_rows = []
        
        # Group by product and date, where multiple stores have the same product
        for (product, date_val), group in df.groupby(["name", "date"]):
            if len(group) >= 2:  # Only create consensus for products with 2+ sources
                consensus_price = np.mean(group["price"])
                price_sources = ", ".join(sorted(group["store"].unique()))
                
                consensus_rows.append({
                    "date": date_val,
                    "store": "Consensus",
                    "sku": "CONSENSUS",
                    "name": product,
                    "price": round(consensus_price, 2),
                    "division": group.iloc[0]["division"],
                    "province": "Multi-Regional",
                    "source": "consensus",
                    "price_sources": price_sources,
                    "num_sources": len(group),
                    "price_min": group["price"].min(),
                    "price_max": group["price"].max(),
                    "price_std": group["price"].std(),
                    "reliability_weight": len(group) / 5.0  # Higher weight for more sources
                })
        
        return pd.DataFrame(consensus_rows)

    def get_sample_health_report(self) -> Dict[str, Any]:
        """
        Generate a sample health report for demo sources
        """
        return {
            "timestamp": datetime.now().isoformat(),
            "sources": {
                "Demo Data Generator": {
                    "health": "healthy",
                    "success_rate": 1.0,
                    "reliability_weight": 1.0,
                    "consecutive_failures": 0,
                    "last_success": datetime.now().isoformat()
                },
                "Coto": {
                    "health": "degraded",
                    "success_rate": 0.3,
                    "reliability_weight": 0.7,
                    "consecutive_failures": 2,
                    "last_success": (datetime.now() - timedelta(hours=2)).isoformat()
                },
                "La Anónima": {
                    "health": "failed",
                    "success_rate": 0.1,
                    "reliability_weight": 0.3,
                    "consecutive_failures": 5,
                    "last_success": (datetime.now() - timedelta(days=1)).isoformat()
                },
                "Jumbo": {
                    "health": "degraded",
                    "success_rate": 0.4,
                    "reliability_weight": 0.7,
                    "consecutive_failures": 1,
                    "last_success": (datetime.now() - timedelta(hours=1)).isoformat()
                }
            },
            "overall_health": "degraded"
        }

# ─────────────────────────────────────────────────────────────────────────
