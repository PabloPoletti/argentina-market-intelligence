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
    WORKING SOURCES ONLY - September 2024 Verified
    Uses ONLY data sources that have been tested and confirmed to work
    """
    try:
        # PRIMARY: Working sources only (no broken scrapers)
        from .working_sources import collect_working_data_only
        from .transform import clean
        
        logger.info("🚀 COLLECTING DATA from WORKING sources only...")
        
        # Use new working sources system
        df = collect_working_data_only()
        df = clean(df)
        
        if not df.empty:
            logger.info(f"✅ WORKING SOURCES SUCCESS: {len(df)} products")
            working_sources = df['source'].unique().tolist()
            logger.info(f"📊 Active Sources: {', '.join(working_sources)}")
        else:
            raise Exception("Working sources returned empty dataset")
        
    except Exception as e:
        logger.error(f"❌ Working sources failed: {e}")
        # NO FALLBACK - fail honestly if sources don't work
        raise Exception("All working data sources failed. No synthetic data fallback.")
    
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
    
    # Insert new data (replace old data)
    try:
        con.execute("DELETE FROM prices")  # Clear old data
        con.execute("INSERT INTO prices SELECT * FROM df")
        logger.info(f"Successfully inserted {len(df)} records into database")
    except Exception as e:
        logger.error(f"Database insertion failed: {e}")
        raise
    
    # Store simple health report
    try:
        sources_count = df.groupby('source').size().to_dict()
        health_report = {
            'timestamp': datetime.now().isoformat(),
            'sources': sources_count,
            'total_products': len(df),
            'status': 'active'
        }
        
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