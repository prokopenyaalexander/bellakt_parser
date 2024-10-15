import logging
import datetime
import os
import re
from symtable import Class

import psycopg2
import requests
from bs4 import BeautifulSoup
from psycopg2 import OperationalError

from config.config_queries import select_all_from_urls_to_crawling_orm, insert_to_urls_to_pricing_products_orm, \
    remove_duplicates_pricing_products_orm, find_duplicates_pricing_products_orm
from config.db_config import create_connection
from config.paths_config import pricing_log_directory
from config.time_config import time_format


log_directory = pricing_log_directory
os.makedirs(log_directory, exist_ok=True)
date = datetime.date.today()
log_file_path = os.path.join(log_directory, f'pricing_data_{date}.log')

logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    filemode='w',
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt=time_format
)

class Pricing:


    def get_pricing_details(self):
        records = select_all_from_urls_to_crawling_orm()
        for row in records:
            logging.info(f"Working with {row[0]} category.")
            try:
                response = requests.get(row[0])
                response.raise_for_status()
            except requests.RequestException as e:
                logging.error(f"Error fetching URL {row[0]}: {e}")
                continue

            soup = BeautifulSoup(response.text, "html.parser")
            pattern = r'\b\d{3,}\b'
            sku = re.findall(pattern, row[0])[0]
            products_title = soup.find("div", class_="topic__heading").find("h1").get_text()
            price = soup.find("span", class_="price_value").get_text()
            temp_stock = soup.find("div", class_="button_block").find("span").find("i").get("title")

            if temp_stock == "В корзину":
                stock = "In stock"
            else:
                stock = "OOS"
            url = row[0]  # потому что, нужно использовать параметризацию.
            date_of_insertion = datetime.date.today()
            insert_to_urls_to_pricing_products_orm(sku, products_title, price, stock, url, date_of_insertion)
    logging.info(f"Finished work")

    def find_duplicates(self):
        find_duplicates_pricing_products_orm()

    def remove_duplicates(self):
        remove_duplicates_pricing_products_orm()


obj = Pricing()
obj.get_pricing_details()

