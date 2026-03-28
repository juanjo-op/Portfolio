# Brazilian E-Commerce Analytics

End-to-end data analytics project using the 
[Olist Brazilian E-Commerce public dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) 
from Kaggle.

## Project Overview

This project demonstrates a full data analytics pipeline — from raw data 
ingestion through cleaning, exploratory analysis, and export — culminating 
in an interactive Power BI dashboard with 7 pages and 25+ visuals.

## Tech Stack

- **Python** (pandas, matplotlib, seaborn) — data pipeline
- **Jupyter Notebooks** — analysis and documentation
- **Power BI Desktop** — dashboard and data modeling
- **Git / GitHub** — version control

## Repository Structure

\```
Brazilian E-Commerce/
├── notebooks/
│   ├── BRNB_1_load_profile_tables.ipynb  # Data loading & profiling
│   ├── BRNM_2_Cleaning_data.ipynb        # Data cleaning pipeline
│   ├── BRNB_3_EDA.ipynb                  # Exploratory data analysis
│   └── BRNB_4_Export.ipynb               # Export for Power BI
├── clean_datasets/                        # Cleaned CSVs
├── powerbi_datasets/                      # Star schema exports + .pbix
└── olist_*_dataset.csv                   # Raw data (source: Kaggle)
\```

## Data Pipeline

| Notebook | Description |
|---|---|
| NB1 — Load & Profile | Loads all 8 raw tables, profiles shape/types/nulls |
| NB2 — Cleaning | Applies cleaning flags, handles nulls and duplicates |
| NB3 — EDA | Exploratory analysis with visualizations |
| NB4 — Export | Builds star schema tables for Power BI |

## Data Model

Star schema with 4 dimension tables and 2 fact tables:

- `dim_customers`, `dim_sellers`, `dim_products`, `dim_date`
- `fact_orders`, `fact_items`

## Power BI Dashboard

7-page interactive dashboard covering:

| Page | Content |
|---|---|
| Overview | KPIs, monthly trend, top states |
| Temporal | Order patterns by month, day, and hour |
| Geographic | Orders by Brazilian state |
| Operational | Delivery performance and distribution |
| Satisfaction | Review score analysis by state |
| Productos | Top categories by revenue and volume |
| Payments | Payment type distribution, installments, price vs freight |

## Dataset

The raw dataset is sourced from Kaggle:
[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)