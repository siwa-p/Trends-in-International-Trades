# DE Capstone Project Proposal

## International Trade insights for United States

### Executive Summary

This project aims to analyze and visualize international trade data for the United States, providing actionable insights for policymakers, businesses, and researchers. By leveraging modern data engineering techniques, we will process large datasets, identify key areas of trade, highlight significant trade partners and look for recent trend in data with reference to the implementation of tarrifs on trade partners.

The end product will be a dashboard and a flexible API for stakeholders to access data reliably and accurately.

### Data Sources

- **U.S. Census Bureau International Trade API**: Provides detailed import and export data by country, commodity, and time period.  
  - [API Documentation](https://www.census.gov/data/developers/data-sets/international-trade.html)
- **The United States Department of Agriculture (USDA) Global Agricultural Trade System (GATS) DATA API** provides comprehensive data on agricultural imports and exports. This includes information on commodities, trading partners, quantities, and values over time.  
  - [USDA GATS DATA API Documentation](https://apps.fas.usda.gov/gats/default.aspx)  
- **Eurostat International Trade Database** provides trade data from EU countries.
  - [Eurostat](https://ec.europa.eu/eurostat/web/user-guides/data-browser/api-data-access) has accessible api
- **The United States Trade Representative (USTR) Press Releases** to scrape tarrif announcements among other things

### Project Objectives

- Aggregate and clean international trade datasets from multiple sources.
- Scrape and/or find data for the tarrif announcements and trade deals.
- Analyze trade flows, trends, and the impact of recent tariff policies.
- Identify top commodities and trading partners for the United States.
- Build interactive dashboards for data visualization.
- Develop a RESTful API for flexible data access.

### Proposed Tech Stack

- **Data Ingestion & Processing**: Python Scripts, Pandas
- **Data Storage**: MINIO (Raw data lake) in parquet format.
- **SQL Engine & Table Format**: Apache Spark SQL with Iceberg tables for scalable, reliable, and ACID-compliant data storage and querying
- **Data Transformation & Modeling**: dbt (Data Build Tool) for version-controlled data transformations ( bronze -- silver -- gold)
- **Prefect** Orchestration. Automate the data ingestion and transformation
- **API Development**: FastAPI
- **Dashboard Visualization**: Superset/Streamlit

### Workflow

The project workflow is divided into two main components:

#### 1. Data Ingestion, Processing, and Transformation

- **Containerized Environment**: All core data engineering tools (MinIO, Apache Spark, dbt, Prefect) are orchestrated within a single development container for streamlined deployment and reproducibility.
- **Data Ingestion**: Prefect orchestrates scheduled extraction of raw data from external APIs (U.S. Census Bureau, USDA GATS, Eurostat) and web scraping for tariff announcements.
- **Raw Data Storage**: Ingested data is stored in MinIO as parquet files, forming the raw data lake.
- **Data Processing & Transformation**: Apache Spark processes raw data into iceberg tables, and dbt manages transformations through bronze, silver, and gold layers, ensuring clean, reliable, and analytics-ready tables.

#### 2. API and Dashboard

- **RESTful API**: FastAPI serves as the backend, providing flexible access to processed trade data and analytics.
- **Dashboard Visualization**: Superset or Streamlit connects to gold tables, enabling interactive dashboards for stakeholders to explore trade insights.
