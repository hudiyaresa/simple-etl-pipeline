# ETL Pipeline Project - Pacmann

## Overview

This project involves building an ETL (Extract, Transform, Load) pipeline to process and store data from various sources into a PostgreSQL database. The data sources include sales data, marketing data, and web-scraped articles.

## Data Sources

1. **Sales Data**
   - **Source:** Dockerized PostgreSQL database
   - **Details:** Includes columns such as name, main_category, sub_category, discount_price, and actual_price.

2. **Marketing Data**
   - **Source:** CSV file (`ElectronicsProductsPricingData.csv`)
   - **Details:** Contains product pricing, availability, and condition details for electronics.

3. **Web Scraping Data**
   - **Source:** Detik.com
   - **Details:** Scrapes article titles, categories, publication dates, and URLs.

## Proposed Solution

### Data Extraction
- **Sales Data:** Fetched from PostgreSQL using Docker.
- **Marketing Data:** Loaded from a CSV file.
- **Web Scraping Data:** Extracted using BeautifulSoup.

### Data Transformation
- **Sales Data:** 
  - Drop unnamed columns
  - Clean price fields (remove symbols, convert to numeric)
  - Fill missing values in price fields with 0
- **Marketing Data:** 
  - Remove 'prices.' prefix from column names
  - Normalize availability (e.g., "Yes" to "In Stock")
  - Remove duplicates
- **Web Scraping Data:** 
  - Extract and save article titles, categories, URLs, and publication dates.

### Data Loading
- Load transformed data into PostgreSQL using the `pangres` library for upserts (insert/update).

## Tools & Stack

- **Python Libraries:**
  - `Pandas` for data manipulation
  - `SQLAlchemy` for database interaction
  - `pangres` for upserting data
  - `BeautifulSoup` and `requests` for web scraping
  - `tqdm` for progress bars
  - `luigi` for task orchestration

- **Docker:** Runs the PostgreSQL database in a container.
- **PostgreSQL:** Target database for storing data.

## Pipeline Design

1. **Extract**
   - Sales Data: From PostgreSQL database
   - Marketing Data: From CSV file
   - Web Articles: From Detik.com via web scraping

2. **Transform**
   - Clean and format sales data
   - Normalize and de-duplicate marketing data
   - Extract relevant fields from scraped articles

3. **Load**
   - Insert/Update all transformed data into PostgreSQL

## Pipeline Execution

### Running the ETL Pipeline
1. Start the Docker container with PostgreSQL.
2. Fetch sales data from PostgreSQL using `fetch_sales_data_from_db()`.
3. Load and transform marketing data with `transform_marketing_data()`.
4. Scrape Detik articles with `scrape_detik()` and transform the data.
5. Load all transformed data into PostgreSQL using `load_data_to_db()`.

### Luigi Pipeline Tasks
- **ExtractSalesData:** Fetches and transforms sales data.
- **ExtractMarketingData:** Transforms and saves marketing data.
- **ScrapeDetik:** Scrapes and saves Detik article data.
- **LoadDataToDatabase:** Loads all data into PostgreSQL.
