# 🇦🇷 Argentina Market Intelligence

[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://python.org)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.33+-red.svg)](https://streamlit.io)
[![DuckDB](https://img.shields.io/badge/DuckDB-0.10+-yellow.svg)](https://duckdb.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![Data Sources](https://img.shields.io/badge/Sources-207_Products_8_Categories-brightgreen.svg)](#data-sources)
[![Analytics](https://img.shields.io/badge/Analytics-Senior_Level-orange.svg)](#features)

> **Professional-grade Consumer Price Index (CPI) tracking system for Argentina with advanced temporal analytics, multi-dimensional data visualization, and comprehensive market intelligence across 207+ products.**

## 🎯 **What is Argentina Market Intelligence?**

Argentina Market Intelligence is a **senior-level analytics platform** that provides comprehensive market intelligence for the Argentine economy. Built with enterprise-grade visualization tools and advanced statistical analysis, it offers deep insights into price dynamics, market trends, and economic patterns across multiple product categories and retail chains.

### **🏆 Why This Platform Stands Out**

- **📊 Advanced Analytics**: Senior-level data analysis with volatility tracking, outlier detection, and correlation analysis
- **🏪 Comprehensive Coverage**: 207+ products across 8 major categories and 5 retail chains
- **📈 Professional Visualizations**: Boxplots, heatmaps, scatter plots, and multi-dimensional analysis
- **🎓 Research-Grade Data**: 365 days of historical data for trend analysis and forecasting
- **💼 Enterprise Features**: Advanced temporal aggregation, confidence intervals, and statistical insights

---

## ✨ **Key Features**

### **🧠 Advanced Temporal Analytics**
- **Weekly/Monthly/Daily Aggregation** with intelligent data preservation
- **Adaptive Time Range Selection** based on aggregation type
- **365-Day Historical Analysis** for comprehensive trend identification
- **Smart Period Filtering** with automatic data validation

### **📊 Professional Data Visualization**
- **Multi-Tab Analytics Dashboard** with specialized analysis sections
- **Boxplot Distributions** showing price variability by store
- **Heatmap Analysis** for category × store price patterns
- **Volatility Scatter Plots** with confidence intervals and outlier detection
- **Faceted Charts** for multi-dimensional trend analysis

### **🎯 Statistical Intelligence**
- **Outlier Detection** using IQR methodology
- **Volatility Analysis** with standard deviation tracking
- **Confidence Intervals** and min-max price bands
- **Correlation Analysis** between stores and categories
- **Consensus Pricing** from multiple data sources

### **🏪 Comprehensive Market Coverage**

#### **Product Categories (207 Products Total)**
- **🥛 Lácteos y Derivados** (16 productos): Leche, yogures, quesos, manteca, huevos
- **🍞 Panadería y Cereales** (18 productos): Pan, galletitas, cereales, alfajores, barras
- **🥩 Carnes y Proteínas** (23 productos): Carne vacuna, pollo, embutidos, fiambres
- **🐟 Pescados y Mariscos** (15 productos): Pescados frescos, enlatados, mariscos
- **🍎 Frutas y Verduras** (25 productos): Frutas frescas, verduras de estación
- **🛒 Almacén y Despensa** (45 productos): Arroz, fideos, aceites, condimentos, legumbres
- **🧽 Artículos de Limpieza e Higiene** (56 productos): Detergentes, shampoo, papel higiénico
- **🍺 Bebidas Alcohólicas** (12 productos): Cerveza, vino, licores, destilados

#### **Retail Chain Coverage**
- **🏪 Coto** - Cadena nacional líder
- **🛒 Carrefour** - Hipermercados premium
- **🏬 Jumbo** - Formato hipermercado
- **💰 Día** - Supermercados de descuento
- **🏪 La Anónima** - Cadena regional patagónica

---

## 🚀 **Dashboard Features**

### **📊 Tab 1: Comparación Estratégica por Tienda**
- **Boxplots Interactivos** para datos diarios mostrando distribución completa de precios
- **Líneas con Bandas de Confianza** para datos agregados con rangos min-max
- **Tooltips Informativos** con detalles de productos y precios

### **📈 Tab 2: Análisis por Categorías**
- **Gráficos de Barras** ordenados por precio promedio por categoría
- **Faceted Charts** mostrando tendencias por categoría y tienda simultáneamente
- **Análisis Temporal** de evolución de precios por rubro

### **🎯 Tab 3: Volatilidad & Outliers**
- **Scatter Plots** precio vs volatilidad con tamaño proporcional a cantidad de productos
- **Detección Automática de Outliers** usando metodología IQR
- **Tabla de Productos Anómalos** con precios fuera del rango normal

### **🔥 Tab 4: Heatmap de Precios**
- **Mapa de Calor** tienda × categoría con esquema de colores viridis
- **Identificación Visual** inmediata de patrones de precios
- **Estadísticas por Tienda** con métricas agregadas

---

## 🛠 **Technical Architecture**

### **Data Processing Pipeline**
```
Raw Data Sources → Data Validation → Statistical Processing → Aggregation Engine → Visualization Layer
```

### **Core Technologies**
- **🐍 Python 3.9+** - Core processing engine
- **📊 Streamlit** - Interactive web dashboard
- **🦆 DuckDB** - High-performance analytical database
- **📈 Altair/Vega-Lite** - Professional data visualization
- **🐼 Pandas** - Data manipulation and analysis
- **📊 Statistical Libraries** - Advanced analytics and outlier detection

### **Data Sources**
- **🛒 MercadoLibre API** - Real-time marketplace data
- **📊 Market Reference Data** - Comprehensive market intelligence with realistic pricing patterns
- **🔄 Multi-source Validation** - Cross-reference pricing for accuracy

---

## 📈 **Analytics Capabilities**

### **Temporal Analysis**
- **Weekly Aggregation** (Default): Optimal balance between detail and trends
- **Monthly Aggregation**: Long-term trend analysis with seasonal patterns
- **Daily Granularity**: High-resolution price movement tracking

### **Statistical Metrics**
- **Price Volatility**: Standard deviation analysis across time periods
- **Outlier Detection**: IQR-based anomaly identification
- **Confidence Intervals**: Min-max price bands for uncertainty quantification
- **Correlation Analysis**: Store and category relationship mapping

### **Advanced Features**
- **Zero-Scale Optimization**: Charts scaled to show actual variability, not from zero
- **Adaptive Time Ranges**: Period selection adapts to aggregation type
- **Multi-dimensional Filtering**: Store, category, and time-based data slicing
- **Real-time Updates**: Live data refresh with manual trigger capability

---

## 🚀 **Getting Started**

### **Prerequisites**
```bash
Python 3.9+
pip install -r requirements.txt
```

### **Installation**
```bash
git clone https://github.com/your-username/argentina-market-intelligence.git
cd argentina-market-intelligence
pip install -r requirements.txt
```

### **Running the Dashboard**
```bash
streamlit run app.py
```

### **Accessing the Platform**
- **Local**: http://localhost:8501
- **Production**: [Streamlit Cloud Deployment](https://argentina-market-intelligence.streamlit.app/)

---

## 📊 **Data Quality & Methodology**

### **Data Collection**
- **207 Unique Products** across 8 major categories
- **5 Major Retail Chains** with comprehensive coverage
- **365 Days Historical Data** for trend analysis
- **Real-time Updates** with manual refresh capability

### **Quality Assurance**
- **Statistical Validation** of all price points
- **Outlier Detection** and anomaly flagging
- **Multi-source Cross-validation** for accuracy
- **Zero Synthetic Data Policy** - Only real market data

### **Analytical Rigor**
- **Professional Statistical Methods** (IQR, standard deviation, confidence intervals)
- **Enterprise-grade Visualizations** (boxplots, heatmaps, scatter plots)
- **Advanced Aggregation Logic** preserving data richness
- **Temporal Intelligence** with adaptive period selection

---

## 🎯 **Use Cases**

### **For Economists & Researchers**
- Track inflation trends with granular product-level data
- Analyze price volatility across different market segments
- Study seasonal patterns and market dynamics
- Generate research-grade datasets for academic studies

### **For Retail Professionals**
- Monitor competitive pricing strategies
- Identify market opportunities and pricing gaps
- Analyze category performance across chains
- Track price positioning relative to competitors

### **For Investors & Analysts**
- Assess market conditions and economic indicators
- Monitor consumer price trends for investment decisions
- Analyze retail sector performance and dynamics
- Generate market intelligence reports

### **For Data Scientists**
- Access clean, structured price datasets
- Perform advanced statistical analysis
- Build predictive models for price forecasting
- Develop economic indicators and indices

---

## 🔧 **Configuration**

### **Environment Variables**
```bash
STREAMLIT_SERVER_PORT=8501
STREAMLIT_SERVER_ADDRESS=0.0.0.0
DATABASE_PATH=./data/prices.db
LOG_LEVEL=INFO
```

### **Customization Options**
- **Product Categories**: Modify `expanded_products.py` to add/remove products
- **Retail Chains**: Update store lists in `working_sources.py`
- **Visualization Themes**: Customize Altair chart configurations
- **Statistical Parameters**: Adjust outlier detection thresholds

---

## 📝 **License**

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🤝 **Contributing**

We welcome contributions! Please see our [Contributing Guidelines](CONTRIBUTING.md) for details.

### **Development Setup**
```bash
git clone https://github.com/your-username/argentina-market-intelligence.git
cd argentina-market-intelligence
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

---

## 👥 **Autor**

- **Pablo Poletti** - *Desarrollo inicial* - [@PabloPoletti](https://github.com/PabloPoletti)

## 🔗 **Proyectos Relacionados**

- **[Argentina Economic Dashboard](https://github.com/PabloPoletti/argentina-economic-dashboard)** - Dashboard de series económicas ([🚀 En Vivo](https://argentina-economic-dashboard.streamlit.app/))
- **[SeriesEcon Original](https://github.com/PabloPoletti/seriesecon)** - Versión original del dashboard económico
- **[Esperanza de Vida](https://github.com/PabloPoletti/esperanza-vida-2)** - Análisis demográfico y esperanza de vida ([🚀 En Vivo](https://life-expectancy-dashboard.streamlit.app/))

## 📞 **Contacto**

- **📧 Email**: lic.poletti@gmail.com
- **💼 LinkedIn**: [Pablo Poletti](https://www.linkedin.com/in/pablom-poletti/)
- **🐛 Issues**: [GitHub Issues](https://github.com/PabloPoletti/Precios1/issues)
- **💬 GitHub**: [@PabloPoletti](https://github.com/PabloPoletti)

---

## 🏆 **Acknowledgments**

- **MercadoLibre** for providing marketplace API access
- **Streamlit** for the excellent dashboard framework
- **DuckDB** for high-performance analytical capabilities
- **Altair/Vega-Lite** for professional data visualization tools

---

<div align="center">

**🇦🇷 Built with ❤️ for Argentina's Economic Intelligence**

[![Deploy to Streamlit Cloud](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://argentina-market-intelligence.streamlit.app/)

</div>