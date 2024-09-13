
# ETL Pipeline Project - Pacmann

## Table of Contents

1. [Overview](#overview)
2. [Data Sources](#data-sources)
   - [Sales Data](#sales-data)
   - [Marketing Data](#marketing-data)
   - [Web Scraping Data](#web-scraping-data)
3. [Problems and Data Engineering Solutions](#problems-and-data-engineering-solutions)
   - [Sales Data](#sales-data-1)
   - [Marketing Data](#marketing-data-1)
   - [Scraping Detik Article Data](#scraping-detik-article-data)
4. [Proposed Solution](#proposed-solution)
   - [Data Extraction](#data-extraction)
   - [Data Transformation](#data-transformation)
   - [Data Loading](#data-loading)
5. [Tools & Stack](#tools--stack)
6. [Pipeline Design](#pipeline-design)
7. [Pipeline Execution](#pipeline-execution)
   - [Prerequisites](#prerequisites)
   - [Installation Guide](#installation-guide)
   - [Running crontab for pipeline every 12 hours with WSL Windows](#running-crontab-for-pipeline-every-12-hours-with-wsl-windows)

## Overview

This project involves building an ETL (Extract, Transform, Load) pipeline to process and store data from various sources into a PostgreSQL database. The data sources include sales data, marketing data, and web-scraped articles.

## Data Sources

### Sales Data

- **Source:** Dockerized PostgreSQL database
- **Details:** Includes columns such as name, main_category, sub_category, discount_price, and actual_price.

### Marketing Data

- **Source:** CSV file (`ElectronicsProductsPricingData.csv`)
- **Details:** Contains product pricing, availability, and condition details for electronics.

### Web Scraping Data

- **Source:** Detik.com
- **Details:** Scrapes article titles, categories, publication dates, and URLs.

## Problems and Data Engineering Solutions

### Sales Data

**Problem:**
The Sales Team wants to analyze sales performance from data pulled from PostgreSQL running in Docker. However, the sales data often has inconsistent formats, such as prices with currency symbols and incomplete data. This complicates analysis and can lead to inaccurate results.

**Data Engineering Solutions:**
1. **Data Cleaning and Transformation:**
   - Implement data cleaning processes to remove currency symbols and convert price columns to a consistent numeric format.
   - Handle missing values by filling them with default values or estimates based on historical data or averages.

2. **Data Pipeline Setup:**
   - Create an ETL pipeline to automatically extract sales data from PostgreSQL, clean the data, and load it into a structured storage system.

3. **Data Validation:**
   - Implement data validation to ensure that the data entered into the system is consistent and complete before further processing.

### Marketing Data

**Problem:**
The Marketing Team needs to analyze marketing data stored in CSV files, but the data often contains irrelevant or inconsistent columns. This hinders the analysis process and complicates data interpretation.

**Data Engineering Solutions:**
1. **Data Cleaning and Normalization:**
   - Clean the data by removing irrelevant columns and consolidating similar columns.
   - Normalize values in columns with different formats, such as product condition and availability.

2. **Duplicate Removal:**
   - Implement steps to detect and remove duplicate rows in the data to avoid redundant information.

3. **Data Integration:**
   - Integrate marketing data with other data sources to provide better context and more comprehensive analysis.

### Scraping Detik Article Data

**Problem:**
The Data Science Team needs article data from Detik for news trend and popularity analysis. However, the Detik website frequently changes and updates its structure.

**Data Engineering Solutions:**
1. **Maintenance and Update of Scraping Scripts:**
   - Monitor and update the web scraping script regularly to adapt to changes in the HTML structure of the Detik website.
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
  - Drop unnamed columns.
  - Clean price fields (remove symbols, convert to numeric).
  - Fill missing values in price fields with 0.
- **Marketing Data:**
  - Remove 'prices.' prefix from column names.
  - Normalize availability (e.g., "Yes" to "In Stock").
  - Remove duplicates.
- **Web Scraping Data:**
  - Extract and save article titles, categories, URLs, and publication dates.

### Data Loading

- Load transformed data into PostgreSQL using the `pangres` library for upserts (insert/update).

## Tools & Stack

- **Python Libraries:**
  - `pandas` for data manipulation
  - `sqlalchemy` for database interaction
  - `pangres` for upserting data
  - `beautifulsoup4` for web scraping
  - `requests` for HTTP requests
  - `psycopg2-binary` and `psycopg2` for PostgreSQL connection
  - `python-dotenv` for environment variable management
  - `datetime` for handling date and time operations
  - `luigi` for task orchestration
  - `os` for operating system interactions

- **Docker:** Runs the PostgreSQL database in a container.
- **PostgreSQL:** Target database for storing data.

## Pipeline Design

![Simple ETL Pipeline](Simple%20ETL%20Pipeline.png)

1. **Extract**
   - Sales Data: From PostgreSQL database
   - Marketing Data: From CSV file
   - Web Articles: From Detik.com via web scraping

2. **Validate**
   - **Sales Data:** Check for inconsistencies, missing values, remove blank columns and correct formats.
   - **Marketing Data:** Ensure data integrity, format consistency, remove blank columns and correctness.
   - **Web Scraping Data:** Validate the completeness and accuracy of the scraped articles.

3. **Transform**
   - **Sales Data:**
     - Drop unnamed columns
     - Clean price fields (remove symbols, convert to numeric)
     - Fill missing values in price fields with 0
   - **Marketing Data:**
     - Remove 'prices.' prefix from column names
     - Normalize availability (e.g., "Yes" to "In Stock")
     - Remove duplicates
   - **Web Scraping Data:**
     - Extract and save article titles, categories, URLs, and publication dates

4. **Load**
   - **Sales Data:** Insert/Update into PostgreSQL
   - **Marketing Data:** Insert/Update into PostgreSQL
   - **Web Scraping Data:** Insert/Update into PostgreSQL

## Pipeline Execution

### Prerequisites

Ensure the following are installed on your system:

- **Docker**
- **Python 3.8+**
- **Luigi**

### Installation Guide

1. **Clone the repository:**
   ```bash
   git clone https://github.com/hudiyaresa/simple-etl-pipeline.git
   cd repo-name
   ```

2. **Run Docker Compose:** Go to the Docker folder and start the container:
   ```bash
   cd data-source/docker-db/
   docker-compose up
   ```

3. **Install required Python packages:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Run Luigi UI:** Optionally, run Luigi's UI on port 9000:
   ```bash
   luigid --port 9000
   ```
   Access it at: [http://localhost:9000](http://localhost:9000)

### Running crontab for Pipeline Every 12 Hours with WSL Windows

1. **Open WSL Terminal:** Ensure `cron` is installed:
   ```bash
   sudo apt-get install cron
   ```

2. **Edit crontab:** Open the crontab editor:
   ```bash
   crontab -e
   ```

3. **Add jobs that run every 12 hours:** Add the following line:
   ```bash
   0 */12 * * * /usr/bin/python3 /path/to/your/pipeline.py
   ```

   This crontab will run the pipeline every 12 hours. Replace the path with the location of your Python script `pipeline.py`.