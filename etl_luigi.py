import luigi
import pandas as pd
from engine import fetch_sales_data_from_docker, transform_sales_data, transform_marketing_data, scrape_detik, dump_sales_data_to_db, setup_db_connection

class ExtractSalesData(luigi.Task):
    def output(self):
        return luigi.LocalTarget('transformed_sales_data.csv')

    def run(self):
        sales_data = fetch_sales_data_from_docker()  # Fetch from Docker DB
        transformed_sales_data = transform_sales_data(sales_data)
        transformed_sales_data.to_csv(self.output().path, index=False)

class ExtractMarketingData(luigi.Task):
    def output(self):
        return luigi.LocalTarget('transformed_marketing_data.csv')

    def run(self):
        marketing_data = pd.read_csv('marketing_data.csv')
        transformed_marketing_data = transform_marketing_data(marketing_data)
        transformed_marketing_data.to_csv(self.output().path, index=False)

class ScrapeDetik(luigi.Task):
    def output(self):
        return luigi.LocalTarget('scraped_detik_data.csv')

    def run(self):
        scraped_data = scrape_detik()
        scraped_data.to_csv(self.output().path, index=False)

class LoadDataToDatabase(luigi.Task):
    def requires(self):
        return [ExtractSalesData(), ExtractMarketingData(), ScrapeDetik()]

    def run(self):
        # Setup the database connection
        db_engine = setup_db_connection('.env.etl_de')

        # Load the data
        sales_data = pd.read_csv(self.input()[0].path)
        marketing_data = pd.read_csv(self.input()[1].path)
        scraped_data = pd.read_csv(self.input()[2].path)

        # Dump into the new database
        dump_sales_data_to_db(sales_data, 'amazon_sales_data', db_engine)
        dump_sales_data_to_db(marketing_data, 'marketing_data', db_engine)
        dump_sales_data_to_db(scraped_data, 'scraped_articles', db_engine)

if __name__ == '__main__':
    luigi.run()
