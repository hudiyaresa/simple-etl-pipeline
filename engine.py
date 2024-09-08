import os
import requests
import datetime
import pandas as pd
from bs4 import BeautifulSoup
from pangres import upsert
from sqlalchemy import create_engine
from tqdm import tqdm
from dotenv import load_dotenv
import subprocess
import time
import json

# # Step 1: Run Docker Compose
# def get_container_name(service_name):
#     # Fetch the container names
#     result = subprocess.run(['docker', 'ps', '--filter', f'name={service_name}', '--format', '{{.Names}}'], stdout=subprocess.PIPE)
#     container_names = result.stdout.decode().strip().split('\n')
    
#     # Debugging output
#     print(f"Container names found: {container_names}")
    
#     if not container_names or container_names[0] == '':
#         raise ValueError(f"No running container found for service: {service_name}")
    
#     return container_names[0]

# def run_docker_compose():
#     print("Starting Docker Compose...")
#     process = subprocess.Popen(["docker-compose", "up", "-d"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
#     stdout, stderr = process.communicate()
    
#     if process.returncode != 0:
#         raise RuntimeError(f"Error starting Docker Compose: {stderr.decode().strip()}")
    
#     time.sleep(10)  # Wait for 10 seconds to ensure containers are up
#     print("Docker Compose started.")

# # Step 2: Check Docker Container Health
# def check_docker_health(service_name):
#     container_name = get_container_name(service_name)
    
#     if not container_name:
#         raise ValueError(f"Container name for service '{service_name}' is empty.")
    
#     print(f"Checking Docker health for container: {container_name}...")
#     while True:
#         try:
#             # Inspect the container to get health status
#             result = subprocess.run(['docker', 'inspect', container_name], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
#             if result.returncode != 0:
#                 raise RuntimeError(f"Error inspecting the container: {result.stderr.decode().strip()}")
            
#             container_info = json.loads(result.stdout)
#             state = container_info[0].get('State', {})
#             health_status = state.get('Health', {}).get('Status', 'unknown')

#             if health_status == "healthy":
#                 print(f"Docker container '{container_name}' is healthy!")
#                 break
#             elif health_status == "unhealthy":
#                 print(f"Docker container '{container_name}' is unhealthy. Exiting...")
#                 break
#             else:
#                 print(f"Docker container '{container_name}' health status is '{health_status}'. Waiting...")
#                 time.sleep(5)
#         except (IndexError, KeyError, json.JSONDecodeError) as e:
#             print(f"Error inspecting the container: {container_name}. Exception: {e}. Retrying...")
#             time.sleep(5)
#         except RuntimeError as e:
#             print(e)
#             time.sleep(5)

# Step 3: Fetch Sales Data from PostgreSQL
def fetch_sales_data_from_docker():
    """
    Fetch sales data from a Dockerized PostgreSQL instance using the .env.docker file.
    """
    load_dotenv('.env.docker')  # Load credentials for Dockerized DB
    DB_USERNAME = os.getenv("WAREHOUSE_DB_USERNAME")
    DB_PASSWORD = os.getenv("WAREHOUSE_DB_PASSWORD")
    DB_HOST = os.getenv("WAREHOUSE_DB_HOST")
    DB_PORT = os.getenv("WAREHOUSE_DB_PORT")
    DB_NAME = os.getenv("WAREHOUSE_DB_NAME")

    # Debugging output
    print(f"DB_USERNAME: {DB_USERNAME}")
    print(f"DB_PASSWORD: {DB_PASSWORD}")
    print(f"DB_HOST: {DB_HOST}")
    print(f"DB_PORT: {DB_PORT}")
    print(f"DB_NAME: {DB_NAME}")

    if not all([DB_USERNAME, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME]):
        raise ValueError("One or more environment variables are missing or empty")

    engine = create_engine(f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    query = "SELECT * FROM public.amazon_sales_data"
    sales_data = pd.read_sql(query, engine)

    print("Sales data successfully fetched from Dockerized PostgreSQL.")
    return sales_data

# Step 4: Transform Sales Data
def transform_sales_data(data, output_file='transformed_sales_data.csv'):
    """
    Transforms the sales data by cleaning up the columns, converting price fields to numeric,
    and handling missing or invalid values.
    """
    print("Transforming sales data...")
    # Drop unnamed columns
    unnamed_cols = [col for col in data.columns if 'Unnamed' in col]
    if unnamed_cols:
        data = data.drop(columns=unnamed_cols)

    # Rename columns for better clarity
    data = data.rename(columns={'discount_price': 'discount_price_INR', 'actual_price': 'actual_price_INR'})

    # Clean and convert price columns
    # Use logging to check the values
    print(data[['discount_price_INR', 'actual_price_INR']].head())

    # Remove symbols and convert to numeric
    data['discount_price_INR'] = data['discount_price_INR'].replace(r'[\$,]', '', regex=True)
    data['discount_price_INR'] = pd.to_numeric(data['discount_price_INR'], errors='coerce')

    data['actual_price_INR'] = data['actual_price_INR'].replace(r'[\$,]', '', regex=True)
    data['actual_price_INR'] = pd.to_numeric(data['actual_price_INR'], errors='coerce')

    # Fill missing values with 0 for numeric columns
    data['discount_price_INR'] = data['discount_price_INR'].fillna(0)
    data['actual_price_INR'] = data['actual_price_INR'].fillna(0)

    # Save transformed data to CSV
    data.to_csv(output_file, index=False)
    print(f"Sales data successfully transformed. Output saved in {output_file}")
    return data

# Step 5: Transform Marketing Data
def transform_marketing_data(data, output_file='transformed_marketing_data.csv'):
    print("Transforming marketing data...")
    # Remove 'prices.' from column names
    data.columns = data.columns.str.replace('prices.', '', regex=True)

    # Normalize 'condition' values
    data['condition'] = data['condition'].replace({
        'new': 'New', 
        'New': 'New',
        'new': 'New', 
        'New (open box)': 'New',
        'Used': 'Used',
        'pre-owned': 'Used'
    })

    # Ubah nilai pada kolom 'condition' jika panjang > 40 karakter
    data['condition'] = data['condition'].apply(lambda x: 'New' if len(str(x)) > 40 else x)

    # Normalize 'availability' values
    data['availability'] = data['availability'].replace({
        'Yes': 'In Stock', 
        'yes': 'In Stock', 
        '32 available': 'In Stock',
        '7 available': 'In Stock',
        'TRUE': 'In Stock',
        'FALSE': 'Out Of Stock',
        'sold': 'Out Of Stock',
        'No': 'Out Of Stock',
        'More on the Way': 'Out Of Stock'
    })

    # Remove duplicate rows
    data.drop_duplicates(inplace=True)

    # Remove unnecessary columns
    columns_to_remove = [col for col in data.columns if 'Unnamed' in col]
    if columns_to_remove:
        data.drop(columns=columns_to_remove, inplace=True)

    # Save transformed data to CSV
    data.to_csv(output_file, index=False)
    print(f"Marketing data successfully transformed. Output saved in {output_file}")
    return data

# Step 6: Web Scraping Detik Articles
def scrape_detik(output_file='scraped_detik_data.csv'):
    """
    Scrapes Detik articles, extracting titles, categories, source URLs, and publication dates.
    """
    print("Scraping Detik articles...")
    url = 'https://www.detik.com/terpopuler'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    articles = soup.find_all('article', class_='list-content__item')
    scraped_data = []

    for article in articles:
        title_element = article.find('h3')
        title = title_element.text.strip() if title_element else 'No Title'

        category_element = article.find('div', class_='media__date')
        category = category_element.text.split(' | ')[0].strip() if category_element else 'No Category'

        source_url_element = article.find('a')
        source_url = source_url_element['href'] if source_url_element else 'No URL'

        span_element = article.find('span', d_time=True)
        date_article = span_element['title'] if span_element and 'title' in span_element.attrs else 'No Date'

        date_scrape = datetime.datetime.now()

        scraped_data.append({
            'title': title,
            'category': category,
            'source_url': source_url,
            'date_article': date_article,
            'date_scrape': date_scrape
        })

    df = pd.DataFrame(scraped_data)
    df.to_csv(output_file, index=False)
    print(f"Articles successfully scraped. Output saved in {output_file}")
    return df

# # Step 7: Load Data to PostgreSQL with Upsert
# def load_data_to_db(transformed_data, table_name, db_engine):
#     print(f"Loading {table_name} data to PostgreSQL...")
#     engine = db_engine()

#     # Using tqdm to track the progress
#     for i in tqdm(range(len(transformed_data)), desc=f"Loading {table_name} data into DB"):
#         upsert(engine=engine, df=transformed_data.iloc[[i]], table_name=table_name, if_row_exists='update')

#     print(f"Success: {table_name} data loaded into PostgreSQL")

# Step 7: Load Data to PostgreSQL with Upsert
def load_data_to_db(transformed_data, table_name, db_engine):
    """
    Loads the transformed data into a PostgreSQL database using pangres upsert.
    Ensures that the DataFrame has named index levels or resets the index.
    """
    print(f"Loading {table_name} data to PostgreSQL...")
    engine = db_engine()

    # Ensure that all index levels are named or reset the index
    if transformed_data.index.name is None:
        # Reset index to remove unnamed index levels
        transformed_data = transformed_data.reset_index(drop=True)
    else:
        # Name the index if it's not named
        transformed_data.index.name = 'id'

    # Perform the upsert operation using pangres
    upsert(engine, transformed_data, table_name=table_name, if_row_exists='update', create_table=True)

    print(f"Success: {table_name} data loaded into PostgreSQL")

# Step 8: Setup Database Connection
def setup_db_connection():
    load_dotenv()
    WAREHOUSE_DB_USERNAME = os.getenv("WAREHOUSE_DB_USERNAME")
    WAREHOUSE_DB_PASSWORD = os.getenv("WAREHOUSE_DB_PASSWORD")
    WAREHOUSE_DB_HOST = os.getenv("WAREHOUSE_DB_HOST")
    WAREHOUSE_DB_PORT = os.getenv("WAREHOUSE_DB_PORT")
    WAREHOUSE_DB_NAME = os.getenv("WAREHOUSE_DB_NAME")

    def dw_db_engine():
        return create_engine(f"postgresql://{WAREHOUSE_DB_USERNAME}:{WAREHOUSE_DB_PASSWORD}@{WAREHOUSE_DB_HOST}:{WAREHOUSE_DB_PORT}/{WAREHOUSE_DB_NAME}")

    return dw_db_engine

# Main Function to Run All Steps
def main():
    # # Step 1: Run Docker Compose for the required services
    # run_docker_compose()

    # # Step 2: Check Docker container health status
    # check_docker_health("postgres")

    # Step 3: Fetch and transform sales data from PostgreSQL
    db_engine = setup_db_connection()
    sales_data = fetch_sales_data_from_docker()
    transformed_sales_data = transform_sales_data(sales_data)

    # Step 4: Load and transform marketing data
    marketing_data = pd.read_csv('ElectronicsProductsPricingData.csv')
    transformed_marketing_data = transform_marketing_data(marketing_data)

    # Step 5: Scrape Detik articles
    scraped_data = scrape_detik()

    # Step 6: Load transformed data into PostgreSQL
    db_engine_new = setup_db_connection('.env.etl_de')
    load_data_to_db(transformed_sales_data, 'amazon_sales_data', db_engine_new)
    load_data_to_db(transformed_marketing_data, 'marketing_data', db_engine_new)
    load_data_to_db(scraped_data, 'scraped_articles', db_engine_new)

if __name__ == "__main__":
    main()