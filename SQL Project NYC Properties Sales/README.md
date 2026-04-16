# SQL Project — NYC Property Sales

Data cleaning and exploration project using NYC property sales data, 
developed in SQL Server (T-SQL).

## Project Overview

This project simulates a real-world data analyst workflow where a client 
requests insights from a raw dataset of New York City property sales. 
The project covers the full pipeline from raw data ingestion through 
cleaning, transformation, and exploratory analysis.

## Tech Stack

- **SQL Server** (T-SQL) — data cleaning and analysis
- **Excel** — raw data source files

## Dataset

Two Excel files imported into SQL Server as separate tables:

| Table | Description |
|---|---|
| `properties_NYC` | Property characteristics (address, units, building class, zip code) |
| `sales_NYC` | Sales transactions (sale date, sale price, tax class) |

The tables are joined on `F1` and `ADDRESS` columns since no formal 
primary key exists across both sources.

## Data Cleaning Steps

| Step | Description |
|---|---|
| Apartment number | Extracted from address field, corrected formatting issues, filled blanks |
| Address | Split apartment number out of address string into separate column |
| Sale date | Converted from text to `date` datatype |
| Sale price | Converted from text to `bigint` datatype |
| Zip code | Replaced zero values with `N/A` |
| Unit counts | Handled zero values across residential, commercial, and total units |
| Tax class | Replaced blank values with `N/A` |
| Duplicate rows | Identified with `ROW_NUMBER()` window function, removed via temp table |
| Unused columns | Dropped `EASE-MENT` column (entirely empty) |
| Building age | Calculated age at moment of sale from `YEAR BUILT` and sale date |

## Exploratory Analysis

After cleaning, a temp table `#Temp_PropertiesSales_NYC` was created 
with the deduplicated, joined dataset. The following business questions 
were answered:

| Question | Technique used |
|---|---|
| Total and average spend per borough and neighborhood | `SUM`, `AVG` with window functions |
| Land area per unit of property | Calculated field (`LandSquareFeet / TotalUnits`) |
| Properties with more than 50 units and their average price | `GROUP BY` + `HAVING` |
| Average number of units per building class category | `GROUP BY` + `AVG` |
| Buildings built after 2010, tax class 2, price under $150K | Multi-condition `WHERE` filter |
| Top 5 most expensive properties sold per borough | `RANK()` window function with `PARTITION BY` |

## How to Run

1. Import `NYC_Property.xlsx` into SQL Server as `properties_NYC`
2. Import `NYC_Sales.xlsx` into SQL Server as `sales_NYC`
3. Both tables should be created under the `Portfolio.dbo` schema
4. Execute `NYC_property_sales_cleaning_and_exploration.sql` top to bottom
