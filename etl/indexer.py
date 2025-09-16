import duckdb
import pandas as pd
import asyncio
import json
import logging
from datetime import datetime

# Configure logging
logger = logging.getLogger(__name__)

def update_all_sources(db_path="data/prices.duckdb"):
    """
    REAL DATA ONLY - No synthetic data allowed
    Comprehensive real-time data collection from multiple Argentine sources
    """
    try:
        # PRIMARY: Real data sources collector
        from .real_data_sources import collect_real_data_only
        from .transform import clean
        
        logger.info("🚀 COLLECTING REAL DATA from multiple Argentine sources...")
        
        # Use asyncio to run comprehensive real data collection
        loop = asyncio.get_event_loop()
        df = loop.run_until_complete(collect_real_data_only())
        df = clean(df)
        
        if not df.empty:
            logger.info(f"✅ REAL DATA SUCCESS: {len(df)} products from online sources")
            logger.info("📊 Sources: CheSuper, PreciosHoy, SeguiPrecios, Government, MercadoLibre, Direct Retailers")
        else:
            raise Exception("Real data collection returned empty dataset")
        
    except Exception as e:
        logger.error(f"❌ REAL DATA COLLECTION FAILED: {e}")
        logger.info("🔄 Trying secondary real data sources...")
        
        try:
            # FALLBACK 1: Modern scrapers (still real data)
            from .modern_scrapers import modern_scrape_all
            from .transform import clean
            
            df = loop.run_until_complete(modern_scrape_all())
            df = clean(df)
            
            if not df.empty:
                logger.info(f"✅ FALLBACK SUCCESS: {len(df)} real products from modern scrapers")
            else:
                raise Exception("Modern scrapers returned no data")
                
        except Exception as modern_error:
            logger.warning(f"Modern scrapers failed: {modern_error}")
            
            try:
                # FALLBACK 2: Original scrapers (still real data)
                from .scrapers import scrape_all
                from .transform import clean
                df = clean(scrape_all())
                
                if not df.empty:
                    logger.info(f"✅ ORIGINAL SCRAPERS SUCCESS: {len(df)} real products")
                else:
                    raise Exception("Original scrapers returned no data")
                    
            except Exception as original_error:
                logger.error(f"❌ ALL REAL DATA SOURCES FAILED")
                logger.error(f"Real data error: {original_error}")
                logger.error(f"Modern scraper error: {modern_error}")
                logger.error(f"Primary source error: {e}")
                
                # NO DEMO DATA - FAIL COMPLETELY if no real data
                raise Exception(
                    "CRITICAL: Unable to collect ANY real price data from online sources. "
                    "All scrapers failed. Check internet connection and source availability."
                )
    
    # Save to database
    con = duckdb.connect(db_path)
    
    # Enhanced table schema with new fields
    con.execute("""
        CREATE TABLE IF NOT EXISTS prices (
            date        DATE,
            store       VARCHAR,
            sku         VARCHAR,
            name        VARCHAR,
            price       DOUBLE,
            division    VARCHAR,
            province    VARCHAR,
            source      VARCHAR DEFAULT 'legacy',
            price_sources VARCHAR DEFAULT NULL,
            num_sources INTEGER DEFAULT 1,
            price_min   DOUBLE DEFAULT NULL,
            price_max   DOUBLE DEFAULT NULL,
            price_std   DOUBLE DEFAULT NULL,
            reliability_weight DOUBLE DEFAULT 1.0
        )
    """)
    
    # Ensure DataFrame has all required columns
    required_columns = [
        'date', 'store', 'sku', 'name', 'price', 'division', 'province',
        'source', 'price_sources', 'num_sources', 'price_min', 'price_max', 
        'price_std', 'reliability_weight'
    ]
    
    # Add missing columns with default values
    for col in required_columns:
        if col not in df.columns:
            if col == 'source':
                df[col] = 'legacy'
            elif col == 'num_sources':
                df[col] = 1
            elif col == 'reliability_weight':
                df[col] = 1.0
            else:
                df[col] = None
    
    # Reorder columns to match table schema
    df = df[required_columns]
    
    # Insert new data
    try:
        con.execute("INSERT INTO prices SELECT * FROM df")
        logger.info(f"Successfully inserted {len(df)} records into database")
    except Exception as e:
        logger.error(f"Database insertion failed: {e}")
        raise
    
    # Store aggregation metadata
    try:
        from .smart_aggregator import PriceAggregator
        aggregator = PriceAggregator()
        health_report = aggregator.get_source_health_report()
        
        # Save health report to separate table
        con.execute("""
            CREATE TABLE IF NOT EXISTS source_health (
                timestamp TIMESTAMP,
                report JSON
            )
        """)
        
        con.execute("""
            INSERT INTO source_health (timestamp, report) 
            VALUES (?, ?)
        """, (datetime.now(), json.dumps(health_report)))
        
        logger.info("Source health report saved")
        
    except Exception as e:
        logger.warning(f"Failed to save health report: {e}")
    
    con.close()

def compute_indices(df: pd.DataFrame) -> pd.DataFrame:
    """
    Índice simple global (sin diferenciación provincial).
    Base = primer día disponible, base=100.
    """
    # 1) Agrupo por fecha, precio medio
    daily = (
        df.groupby("date")
          .price.mean()
          .reset_index(name="avg_price")
          .sort_values("date")
    )

    # 2) Si no hay datos, devuelvo DataFrame vacío con columnas esperadas
    if daily.empty:
        return pd.DataFrame(columns=["date", "avg_price", "index"])

    # 3) Base = primer avg_price
    base = daily.iloc[0]["avg_price"]
    if base == 0 or pd.isna(base):
        # Evito división por cero
        daily["index"] = pd.NA
    else:
        daily["index"] = daily["avg_price"] / base * 100

    return daily
