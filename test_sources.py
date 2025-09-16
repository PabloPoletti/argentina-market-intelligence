#!/usr/bin/env python3
"""
Test script to verify which data sources are actually working
"""
import asyncio
import sys
import os
sys.path.append('.')

async def test_sources():
    try:
        from etl.verified_sources import collect_verified_data_only
        print("🔍 Testing VERIFIED sources...")
        data = await collect_verified_data_only()
        print(f"✅ SUCCESS: {len(data)} products collected")
        print(f"Sources: {data['source'].unique().tolist()}")
        print(f"Sample data:")
        print(data.head())
        return True
    except Exception as e:
        print(f"❌ VERIFIED sources FAILED: {e}")
        return False

async def test_real_sources():
    try:
        from etl.real_data_sources import collect_real_data_only
        print("\n🔍 Testing REAL sources...")
        data = await collect_real_data_only()
        print(f"✅ SUCCESS: {len(data)} products collected")
        print(f"Sources: {data['source'].unique().tolist()}")
        return True
    except Exception as e:
        print(f"❌ REAL sources FAILED: {e}")
        return False

async def main():
    print("🚀 AUDIT: Testing all data sources to see which actually work...")
    
    verified_works = await test_sources()
    real_works = await test_real_sources()
    
    print("\n📊 RESULTS:")
    print(f"✅ Verified sources: {'WORKING' if verified_works else 'FAILED'}")
    print(f"✅ Real sources: {'WORKING' if real_works else 'FAILED'}")

if __name__ == "__main__":
    asyncio.run(main())
