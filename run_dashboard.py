#!/usr/bin/env python3
"""
Argentina Market Intelligence - Dashboard Launcher
==================================================

Convenience script to launch the Argentina Market Intelligence dashboard
with optimal configuration and error handling.

Usage:
    python run_dashboard.py [options]

Options:
    --port PORT     Set custom port (default: 8501)
    --debug         Enable debug mode with verbose logging
    --dev           Development mode with auto-reload
    --no-browser    Don't auto-open browser

Example:
    python run_dashboard.py --port 8080 --debug
"""

import subprocess
import sys
import argparse
import webbrowser
import time
from pathlib import Path

def check_requirements():
    """Check if required packages are installed."""
    try:
        import streamlit
        import pandas
        import duckdb
        import playwright
        print("✅ All required packages are installed")
        return True
    except ImportError as e:
        print(f"❌ Missing required package: {e}")
        print("📦 Install with: pip install -r requirements.txt")
        return False

def ensure_playwright():
    """Ensure Playwright browsers are installed."""
    try:
        print("🔧 Checking Playwright browsers...")
        result = subprocess.run(
            ["playwright", "install", "chromium", "--dry-run"],
            capture_output=True,
            text=True
        )
        
        if "chromium" not in result.stdout.lower():
            print("📥 Installing Playwright Chromium browser...")
            subprocess.run(["playwright", "install", "chromium"], check=True)
            print("✅ Playwright Chromium installed successfully")
        else:
            print("✅ Playwright Chromium already installed")
            
    except FileNotFoundError:
        print("❌ Playwright not found. Install with: pip install playwright")
        return False
    except subprocess.CalledProcessError as e:
        print(f"❌ Error installing Playwright: {e}")
        return False
    
    return True

def create_data_directory():
    """Create data directory if it doesn't exist."""
    data_dir = Path("data")
    if not data_dir.exists():
        data_dir.mkdir(exist_ok=True)
        print("📁 Created data directory")

def main():
    """Main launcher function."""
    parser = argparse.ArgumentParser(
        description="Launch Argentina Market Intelligence Dashboard"
    )
    parser.add_argument(
        "--port", 
        type=int, 
        default=8501, 
        help="Port to run the dashboard on (default: 8501)"
    )
    parser.add_argument(
        "--debug", 
        action="store_true", 
        help="Enable debug mode"
    )
    parser.add_argument(
        "--dev", 
        action="store_true", 
        help="Development mode with auto-reload"
    )
    parser.add_argument(
        "--no-browser", 
        action="store_true", 
        help="Don't auto-open browser"
    )
    
    args = parser.parse_args()
    
    print("🇦🇷 Argentina Market Intelligence Dashboard")
    print("=" * 50)
    
    # Check requirements
    if not check_requirements():
        sys.exit(1)
    
    # Setup environment
    if not ensure_playwright():
        sys.exit(1)
    
    create_data_directory()
    
    # Build Streamlit command
    cmd = [
        "streamlit", "run", "app.py",
        "--server.port", str(args.port),
        "--server.enableCORS", "false",
        "--server.enableXsrfProtection", "false"
    ]
    
    if args.debug:
        cmd.extend(["--logger.level", "debug"])
    
    if args.dev:
        cmd.extend(["--server.runOnSave", "true"])
    
    if args.no_browser:
        cmd.extend(["--server.headless", "true"])
    
    # Launch dashboard
    print(f"🚀 Starting dashboard on port {args.port}...")
    print(f"🌐 Dashboard will be available at: http://localhost:{args.port}")
    
    if not args.no_browser:
        # Open browser after a short delay
        def open_browser():
            time.sleep(3)
            webbrowser.open(f"http://localhost:{args.port}")
        
        import threading
        browser_thread = threading.Thread(target=open_browser)
        browser_thread.daemon = True
        browser_thread.start()
    
    try:
        # Run Streamlit
        subprocess.run(cmd, check=True)
    except KeyboardInterrupt:
        print("\n👋 Dashboard stopped by user")
    except subprocess.CalledProcessError as e:
        print(f"❌ Error running dashboard: {e}")
        sys.exit(1)
    except FileNotFoundError:
        print("❌ Streamlit not found. Install with: pip install streamlit")
        sys.exit(1)

if __name__ == "__main__":
    main()
