# ──────────────────── etl/smart_aggregator.py ────────────────────────────
"""
Smart Price Aggregation System
==============================

This module implements intelligent price aggregation from multiple sources,
including fallback mechanisms, outlier detection, and weighted averaging.

Features:
- Multi-source price collection with fallback
- Outlier detection using statistical methods
- Weighted averaging based on source reliability
- Real-time source health monitoring
- Price validation and quality scoring
"""

import asyncio
import pandas as pd
import numpy as np
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple
import logging
from dataclasses import dataclass
from enum import Enum
import statistics

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SourceHealth(Enum):
    """Source health status enumeration"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    FAILED = "failed"
    UNKNOWN = "unknown"

@dataclass
class SourceMetrics:
    """Metrics for tracking source performance"""
    name: str
    success_rate: float = 0.0
    avg_response_time: float = 0.0
    data_quality_score: float = 0.0
    last_successful_fetch: Optional[datetime] = None
    consecutive_failures: int = 0
    health_status: SourceHealth = SourceHealth.UNKNOWN
    reliability_weight: float = 1.0

class PriceAggregator:
    """
    Intelligent price aggregation system that combines data from multiple sources
    with fallback mechanisms and quality validation.
    """
    
    def __init__(self):
        self.source_metrics: Dict[str, SourceMetrics] = {}
        self.price_history: List[Dict] = []
        self.outlier_threshold = 2.0  # Standard deviations for outlier detection
        self.min_sources_for_consensus = 2
        
        # Initialize source metrics
        self._initialize_source_metrics()
    
    def _initialize_source_metrics(self):
        """Initialize metrics for all known sources"""
        sources = ["Coto", "La Anónima", "Jumbo", "MercadoLibre"]
        for source in sources:
            self.source_metrics[source] = SourceMetrics(
                name=source,
                reliability_weight=1.0
            )
    
    async def fetch_all_sources(self) -> Dict[str, pd.DataFrame]:
        """
        Fetch data from all sources with error handling and timing
        """
        from .scrapers import coto_df, laanonima_df, jumbo_df
        from .ml_scraper import ml_price_stats
        
        results = {}
        
        # Async sources
        async_tasks = {
            "Coto": self._safe_fetch_async(coto_df, "Coto"),
            "Jumbo": self._safe_fetch_async(jumbo_df, "Jumbo")
        }
        
        # Execute async tasks
        async_results = await asyncio.gather(*async_tasks.values(), return_exceptions=True)
        for source, result in zip(async_tasks.keys(), async_results):
            if isinstance(result, Exception):
                logger.error(f"{source} failed: {result}")
                self._update_source_health(source, False)
                results[source] = pd.DataFrame()
            else:
                results[source] = result
                self._update_source_health(source, True)
        
        # Sync sources
        try:
            results["La Anónima"] = laanonima_df()
            self._update_source_health("La Anónima", True)
        except Exception as e:
            logger.error(f"La Anónima failed: {e}")
            self._update_source_health("La Anónima", False)
            results["La Anónima"] = pd.DataFrame()
        
        # MercadoLibre (convert to DataFrame format)
        try:
            ml_data = self._fetch_ml_as_dataframe()
            results["MercadoLibre"] = ml_data
            self._update_source_health("MercadoLibre", len(ml_data) > 0)
        except Exception as e:
            logger.error(f"MercadoLibre failed: {e}")
            self._update_source_health("MercadoLibre", False)
            results["MercadoLibre"] = pd.DataFrame()
        
        return results
    
    async def _safe_fetch_async(self, fetch_func, source_name: str) -> pd.DataFrame:
        """Safely execute async fetch function with timeout"""
        try:
            start_time = datetime.now()
            result = await asyncio.wait_for(fetch_func(), timeout=60)
            response_time = (datetime.now() - start_time).total_seconds()
            
            # Update metrics
            if source_name in self.source_metrics:
                self.source_metrics[source_name].avg_response_time = response_time
            
            return result
        except asyncio.TimeoutError:
            logger.error(f"{source_name} timed out")
            raise
        except Exception as e:
            logger.error(f"{source_name} error: {e}")
            raise
    
    def _fetch_ml_as_dataframe(self) -> pd.DataFrame:
        """Convert MercadoLibre data to standard DataFrame format"""
        from .ml_scraper import ml_price_stats
        
        # Common product queries for MercadoLibre
        queries = [
            "leche entera", "pan francés", "aceite girasol", "arroz", 
            "fideos", "azúcar", "harina", "yogur", "manteca", "queso"
        ]
        
        rows = []
        for query in queries:
            try:
                stats = ml_price_stats(query)
                if stats:
                    rows.append({
                        "date": date.today(),
                        "store": "MercadoLibre",
                        "sku": f"ml_{query.replace(' ', '_')}",
                        "name": query,
                        "price": stats["avg_price"],
                        "division": self._map_ml_division(query),
                        "province": "Nacional",
                        "price_min": stats["min_price"],
                        "price_max": stats["max_price"],
                        "price_source": "aggregated"
                    })
            except Exception as e:
                logger.warning(f"Failed to fetch ML data for {query}: {e}")
                continue
        
        return pd.DataFrame(rows)
    
    def _map_ml_division(self, query: str) -> str:
        """Map MercadoLibre product to division"""
        query = query.lower()
        if any(word in query for word in ["leche", "yogur", "manteca", "queso"]):
            return "Leche, lácteos y huevos"
        elif any(word in query for word in ["pan", "harina", "fideos", "arroz", "azúcar"]):
            return "Alimentos y bebidas no alcohólicas"
        elif "aceite" in query:
            return "Aceites y grasas"
        return "Otros bienes"
    
    def _update_source_health(self, source: str, success: bool):
        """Update source health metrics"""
        if source not in self.source_metrics:
            return
        
        metrics = self.source_metrics[source]
        
        if success:
            metrics.consecutive_failures = 0
            metrics.last_successful_fetch = datetime.now()
            # Update success rate (simple moving average)
            metrics.success_rate = min(1.0, metrics.success_rate + 0.1)
        else:
            metrics.consecutive_failures += 1
            metrics.success_rate = max(0.0, metrics.success_rate - 0.2)
        
        # Determine health status
        if metrics.success_rate > 0.8:
            metrics.health_status = SourceHealth.HEALTHY
            metrics.reliability_weight = 1.0
        elif metrics.success_rate > 0.5:
            metrics.health_status = SourceHealth.DEGRADED
            metrics.reliability_weight = 0.7
        else:
            metrics.health_status = SourceHealth.FAILED
            metrics.reliability_weight = 0.3
    
    def aggregate_prices(self, source_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Intelligently aggregate prices from multiple sources
        """
        all_data = []
        
        for source, df in source_data.items():
            if df.empty:
                continue
            
            # Add source reliability weight
            reliability = self.source_metrics.get(source, SourceMetrics(source)).reliability_weight
            df = df.copy()
            df["source"] = source
            df["reliability_weight"] = reliability
            all_data.append(df)
        
        if not all_data:
            logger.warning("No data from any source")
            return pd.DataFrame()
        
        # Combine all data
        combined_df = pd.concat(all_data, ignore_index=True)
        
        # Group by product (name) and calculate weighted consensus
        aggregated_rows = []
        
        for product_name, group in combined_df.groupby("name"):
            consensus_price = self._calculate_consensus_price(group)
            if consensus_price is not None:
                # Use the first row as template
                row = group.iloc[0].copy()
                row["price"] = consensus_price
                row["source"] = "consensus"
                row["price_sources"] = ", ".join(group["source"].unique())
                row["num_sources"] = len(group)
                
                # Add price range information
                row["price_min"] = group["price"].min()
                row["price_max"] = group["price"].max()
                row["price_std"] = group["price"].std()
                
                aggregated_rows.append(row)
        
        result_df = pd.DataFrame(aggregated_rows)
        
        # Log aggregation summary
        logger.info(f"Aggregated {len(result_df)} products from {len(source_data)} sources")
        for source, metrics in self.source_metrics.items():
            logger.info(f"{source}: {metrics.health_status.value}, reliability: {metrics.reliability_weight:.2f}")
        
        return result_df
    
    def _calculate_consensus_price(self, group: pd.DataFrame) -> Optional[float]:
        """
        Calculate consensus price from multiple sources using weighted average
        with outlier detection
        """
        if len(group) == 0:
            return None
        
        prices = group["price"].values
        weights = group["reliability_weight"].values
        
        # Single source
        if len(prices) == 1:
            return float(prices[0])
        
        # Remove outliers using modified Z-score
        prices_clean, weights_clean = self._remove_outliers(prices, weights)
        
        if len(prices_clean) == 0:
            logger.warning(f"All prices filtered as outliers for product group")
            return float(np.mean(prices))  # Fallback to simple average
        
        # Calculate weighted average
        if np.sum(weights_clean) > 0:
            weighted_avg = np.average(prices_clean, weights=weights_clean)
        else:
            weighted_avg = np.mean(prices_clean)
        
        return float(weighted_avg)
    
    def _remove_outliers(self, prices: np.ndarray, weights: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        """
        Remove outliers using modified Z-score method
        """
        if len(prices) <= 2:
            return prices, weights
        
        # Calculate modified Z-score using median
        median_price = np.median(prices)
        mad = np.median(np.abs(prices - median_price))  # Median Absolute Deviation
        
        if mad == 0:
            return prices, weights
        
        modified_z_scores = 0.6745 * (prices - median_price) / mad
        
        # Keep prices within threshold
        mask = np.abs(modified_z_scores) < self.outlier_threshold
        
        return prices[mask], weights[mask]
    
    def get_source_health_report(self) -> Dict:
        """Generate source health report"""
        report = {
            "timestamp": datetime.now().isoformat(),
            "sources": {},
            "overall_health": "unknown"
        }
        
        healthy_sources = 0
        total_sources = len(self.source_metrics)
        
        for source, metrics in self.source_metrics.items():
            report["sources"][source] = {
                "health": metrics.health_status.value,
                "success_rate": metrics.success_rate,
                "reliability_weight": metrics.reliability_weight,
                "consecutive_failures": metrics.consecutive_failures,
                "last_success": metrics.last_successful_fetch.isoformat() if metrics.last_successful_fetch else None
            }
            
            if metrics.health_status == SourceHealth.HEALTHY:
                healthy_sources += 1
        
        # Overall health
        if healthy_sources >= total_sources * 0.75:
            report["overall_health"] = "healthy"
        elif healthy_sources >= total_sources * 0.5:
            report["overall_health"] = "degraded"
        else:
            report["overall_health"] = "critical"
        
        return report

# Convenience function for backward compatibility
async def scrape_all_smart() -> pd.DataFrame:
    """
    Smart scraping function that replaces the original scrape_all()
    with intelligent aggregation
    """
    aggregator = PriceAggregator()
    source_data = await aggregator.fetch_all_sources()
    return aggregator.aggregate_prices(source_data)

# ─────────────────────────────────────────────────────────────────────────
