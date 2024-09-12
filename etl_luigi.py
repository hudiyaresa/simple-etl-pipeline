import luigi
import pandas as pd
import requests
import datetime
import re
from bs4 import BeautifulSoup
from helper.db_connector import source_db_engine, dw_db_engine
from helper.data_validator import validation_process
from pangres import upsert

# Task to extract sales data from a Dockerized PostgreSQL instance
class ExtractSalesData(luigi.Task):
    def output(self):
        # Output path for the extracted sales data CSV file
        return luigi.LocalTarget('data-source/raw-data/extract_sales_data.csv')

    def run(self):
        # Setup source engine for fetching data from PostgreSQL
        source_engine = source_db_engine()

        # Query to fetch sales data
        query_sales = "SELECT * FROM public.amazon_sales_data"
        extract_sales_data = pd.read_sql(query_sales, con=source_engine)

        # Save extracted sales data to CSV
        extract_sales_data.to_csv(self.output().path, index=False)
        print("Sales data successfully fetched from Dockerized PostgreSQL.")

# Task to extract marketing data from a CSV file
class ExtractMarketingData(luigi.Task):
    def output(self):
        # Output path for the extracted marketing data CSV file
        return luigi.LocalTarget('data-source/raw-data/extract_marketing_data.csv')

    def run(self):
        # Read marketing data from CSV
        extract_marketing_data = pd.read_csv('data-source/csv/ElectronicsProductsPricingData.csv')
        extract_marketing_data.to_csv(self.output().path, index=False)
        print("Marketing data successfully extracted.")

# Task to scrape articles from Detik.com
class ScrapeDetik(luigi.Task):
    def output(self):
        # Output path for the scraped Detik data CSV file
        return luigi.LocalTarget('data-source/raw-data/scraped_detik_data.csv')

    def run(self):
        print("Scraping Detik articles...")
        url = 'https://www.detik.com/terpopuler'

        try:
            response = requests.get(url)
            response.raise_for_status()  # Ensure we notice bad responses
        except requests.RequestException as e:
            print(f"Request failed: {e}")
            return

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

            # Extract date using BeautifulSoup
            span_element = article.find('span', title=True)
            if span_element:
                date_article = span_element.get('title')
                # Hilangkan hari dan koma (contoh: 'Kamis, ')
                date_article = date_article.split(',')[1].strip() if ',' in date_article else 'Invalid Date'
            else:
                date_article = 'Invalid Date'


            # span_element = article.find('span', title=True)
            # date_article = span_element.get('title') if span_element else 'Invalid Date'


            # Extract date using BeautifulSoup
            # span_element = article.find('span', d_time=True)
            # date_article = 'Invalid Date'

            # if span_element:
            #     if 'd-time' in span_element.attrs:
            #         try:
            #             # Ambil timestamp dari atribut 'd-time'
            #             timestamp = int(span_element['d-time'])
            #             date_article = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
            #         except (ValueError, TypeError) as e:
            #             print(f"Error converting timestamp: {e}, value: {span_element['d-time']}")
            #     elif 'title' in span_element.attrs:
            #         try:
            #             # Ambil tanggal dari atribut 'title' jika 'd-time' tidak valid
            #             date_article = datetime.datetime.strptime(span_element['title'], '%A, %d %b %Y %H:%M WIB').strftime('%Y-%m-%d %H:%M:%S')
            #         except ValueError as ve:
            #             print(f"Error parsing date from title: {ve}, value: {span_element['title']}")
            
            date_scrape = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            scraped_data.append({
                'title': title,
                'category': category,
                'source_url': source_url,
                'date_article': date_article,
                'date_scrape': date_scrape
            })

        df = pd.DataFrame(scraped_data)
        df.to_csv(self.output().path, index=False)
        print(f"Articles successfully scraped. Output saved in {self.output().path}")

# Task to validate the extracted and scraped data
class ValidateData(luigi.Task):
    def requires(self):
        # Dependencies: extracted sales, marketing data, and scraped Detik data
        return [ExtractSalesData(), ExtractMarketingData(), ScrapeDetik()]    

    def output(self):
        pass
    
    def run(self):
        # Read sales data for validation
        sales_data = pd.read_csv(self.input()[0].path)

        # Read marketing data for validation
        marketing_data = pd.read_csv(self.input()[1].path)

        # Read Detik scraped data for validation
        detik_data = pd.read_csv(self.input()[2].path)

        # Custom validation function can be applied here
        validation_process(sales_data, "extract_sales_data")
        validation_process(marketing_data, "extract_marketing_data")
        validation_process(detik_data, "scraped_detik_data")

# Task to transform sales data
class TransformSalesData(luigi.Task):
    def requires(self):
        return [ExtractSalesData()]
    
    def output(self):
        # Output path for transformed sales data
        return luigi.LocalTarget("data-transform/transform_sales_data.csv")
    
    def run(self):
        """
        Transforms the sales data by cleaning columns, handling missing values,
        and converting price fields to numeric.
        """
        print("Transforming sales data...")
        sales_data = pd.read_csv(self.input()[0].path)

        # Remove unnamed columns
        sales_data = sales_data.loc[:, ~sales_data.columns.str.contains('^Unnamed')]

        # Rename columns for better understanding
        sales_data = sales_data.rename(columns={'discount_price': 'discount_price_INR', 'actual_price': 'actual_price_INR'})

        # Function to clean price values
        def clean_price(value):
            # Ensure the value is a string; if not, convert it to a string
            if pd.isna(value):
                return None
            value = str(value)
            # Extract digits from the string
            cleaned_value = re.sub(r'[^\d]', '', value)
            return int(cleaned_value) if cleaned_value else None

        # Apply cleaning function to columns
        sales_data['discount_price_INR'] = sales_data['discount_price_INR'].apply(clean_price)
        sales_data['actual_price_INR'] = sales_data['actual_price_INR'].apply(clean_price)

        # impute missing values
        sales_data["discount_price_INR"] = sales_data["discount_price_INR"].fillna(value = "0")
        sales_data["actual_price_INR"] = sales_data["actual_price_INR"].fillna(value = "0")


        # Clean and convert price columns
        # sales_data['discount_price_INR'] = pd.to_numeric(sales_data['discount_price_INR'].str.replace(r'[^\d.]', ''), errors='coerce')
        # sales_data['actual_price_INR'] = pd.to_numeric(sales_data['actual_price_INR'].str.replace(r'[^\d.]', ''), errors='coerce')

        # Fill missing price values with 0
        # sales_data.fillna({'discount_price_INR': 0, 'actual_price_INR': 0}, inplace=True)

        # Save transformed data to CSV
        sales_data.to_csv(self.output().path, index=False)
        print(f"Sales data successfully transformed. Output saved at {self.output().path}")

# Task to transform marketing data
class TransformMarketingData(luigi.Task):
    def requires(self):
        return [ExtractMarketingData()]

    def output(self):
        return luigi.LocalTarget("data-transform/transform_marketing_data.csv")

    def run(self):
        """
        Transforms the marketing data by cleaning column names, normalizing data,
        and handling duplicates.
        """
        print("Transforming marketing data...")
        marketing_data = pd.read_csv(self.input()[0].path)

        # Clean column names
        marketing_data.columns = marketing_data.columns.str.replace('prices.', '', regex=True)

        # Normalize the 'condition' column
        marketing_data['condition'] = marketing_data['condition'].replace({
            'new': 'New', 'New (open box)': 'New', 'pre-owned': 'Used', 'refurbished': 'Refurbished', 'New other (see details)': 'New'
        })

        marketing_data['condition'] = marketing_data['condition'].apply(lambda x: 'New' if len(str(x)) > 40 else x)

        # Normalize the 'availability' column
        marketing_data['availability'] = marketing_data['availability'].replace({
            'Yes': 'In Stock', 'yes': 'In Stock', '7 available': 'In Stock', '32 available': 'In Stock', 'TRUE': 'In Stock', 'No': 'Out Of Stock', 'More on the Way': 'Out Of Stock', 'sold': 'Out Of Stock', 'FALSE': 'Out Of Stock'
        })

        # Normalize the 'shipping' column
        marketing_data['shipping'] = marketing_data['shipping'].replace({
            'Free Shipping for this Item': 'Free Shipping', 'FREE': 'Free Shipping', 'Free Delivery': 'Free Shipping', '32 available': 'In Stock', 'TRUE': 'In Stock', 'No': 'Out Of Stock', 'More on the Way': 'Out Of Stock', 'sold': 'Out Of Stock', 'FALSE': 'Out Of Stock'
        })

        # Remove duplicates
        marketing_data.drop_duplicates(inplace=True)

        # Remove unnecessary columns
        columns_to_remove = [col for col in marketing_data.columns if 'Unnamed' in col]
        if columns_to_remove:
            marketing_data.drop(columns=columns_to_remove, inplace=True)        

        # Save transformed data to CSV
        marketing_data.to_csv(self.output().path, index=False)
        print(f"Marketing data successfully transformed. Output saved at {self.output().path}")

# Task to load transformed data into separate tables in the database
class LoadData(luigi.Task):
    def requires(self):
        return [TransformSalesData(), TransformMarketingData(), ScrapeDetik()]

    def run(self):
        # Setup the database connection
        dw_engine = dw_db_engine()

        # Define table names
        table_names = ['sales_data', 'marketing_data', 'detik_scrape']

        # Define input files
        input_files = self.input()

        for input_file, table_name in zip(input_files, table_names):
            # Read the data from previous task
            data = pd.read_csv(input_file.path)
            
            # Generate an index column
            data.insert(0, 'analysis_id', range(0, len(data)))
            data.set_index('analysis_id', inplace=True)
            
            # Apply upsert
            upsert(con=dw_engine, df=data, table_name=table_name, if_row_exists="update")
            
            # Save the final output as a CSV (optional, if needed for debugging or further processing)
            data.to_csv(f"data-transform/{table_name}.csv", index=False)
            print(f"Data successfully upserted into {table_name} table.")

if __name__ == "__main__":
    luigi.build([LoadData()], local_scheduler=True)