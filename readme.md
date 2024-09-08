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

## Problems and Data Engineering Solutions

### Sales Data

**Problem:**
Sales Team wants to analyze sales performance from data pulled from PostgreSQL running in Docker. However, the sales data often has inconsistent formats, such as prices with currency symbols and incomplete data. This complicates analysis and can lead to inaccurate results.

**Data Engineer Solutions:**
1. **Data Cleaning and Transformation:**
   - The Data Engineer will implement data cleaning processes to remove currency symbols and convert price columns to a consistent numeric format.
   - Handle missing values by filling them with default values or estimates based on historical data or averages.

2. **Data Pipeline Setup:**
   - Create an ETL (Extract, Transform, Load) pipeline to automatically extract sales data from PostgreSQL, clean the data, and load it into a structured storage system.

3. **Data Validation:**
   - Implement data validation to ensure that the data entered into the system is consistent and complete before further processing.

### Marketing Data

**Problem:**
Marketing Team needs to analyze marketing data stored in CSV files, but the data often contains irrelevant or inconsistent columns. This hinders the analysis process and complicates data interpretation.

**Data Engineer Solutions:**
1. **Data Cleaning and Normalization:**
   - The Data Engineer will clean the data by removing irrelevant columns and consolidating similar columns.
   - Normalize values in columns with different formats, such as product condition and availability.

2. **Duplicate Removal:**
   - Implement steps to detect and remove duplicate rows in the data to avoid redundant information.

3. **Data Integration:**
   - Integrate marketing data with other data sources to provide better context and more comprehensive analysis.

### Scraping Detik Article Data

**Problem:**
Data Scientist Team needs article data from Detik for news trend and popularity analysis. However, the Detik website frequently changes and update frequently.

**Data Engineer Solutions:**
1. **Maintenance and Update of Scraping Scripts:**
   - The Data Engineer will monitor and update the web scraping script regularly to adapt to changes in the HTML structure of the Detik website.
   - Implement testing to ensure the scraping script can handle unexpected changes.

2. **Temporary Data Storage:**
   - Implement a temporary storage system to save scraping results and validate data quality before permanently storing it in the database.

3. **Scheduling and Monitoring:**
   - Implement scheduling to run scraping tasks regularly and monitoring to detect issues promptly.


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
