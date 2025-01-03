import datetime
import logging
import os
import psycopg2
import requests
from bs4 import BeautifulSoup
from config.config_queries import (select_all_from_site_set, insert_to_ranking_products, find_duplicates,
                                   remove_duplicates)
from config.paths_config import ranking


log_directory = ranking #~/Documents/projects/profidata/customers/bellakt/logs/ranking_logs
os.makedirs(log_directory, exist_ok=True)
date = datetime.date.today()
log_file_path = os.path.join(log_directory, f'get_count_ranking_products_data_{date}.log')

logger = logging.getLogger('GetCountRankingProductslogger')
logger.setLevel(logging.INFO)
handler = logging.FileHandler(log_file_path, mode='w')
handler.setFormatter(logging.Formatter(fmt='[%(asctime)s: %(levelname)s] %(message)s'))
logger.addHandler(handler)


class RankProds:

    def get_products_count(self):
        records = select_all_from_site_set()
        if records:
            logger.info(f"Data exacted successfully")
        for row in records:
            url = row[1]
            response = requests.get(url)
            soup = BeautifulSoup(response.text, "html.parser")
            if soup.find("div", class_="nums"):
                number_of_pages = int(soup.find("div", class_="nums").find_all("a", class_="dark_link")[-1].text)
                self.process_multiple_pages_data(url, number_of_pages)
            else:
                self.process_single_page_data(url)

    @staticmethod
    def process_multiple_pages_data(url, number_of_pages):
        base_category_url = url
        response = requests.get(base_category_url)
        soup = BeautifulSoup(response.text, "html.parser")
        category_name = soup.find("div", class_='topic__heading').find("h1").get_text()
        count_of_products = 0
        page_param = f'?PAGEN_1='
        urls_to_crawl = [f'{url}{page_param}{gen_url}' for gen_url in range(2, number_of_pages+1)]
        urls_to_crawl.insert(0, url)
        category_url = base_category_url
        for url in urls_to_crawl:
            response = requests.get(url)
            soup = BeautifulSoup(response.text, "html.parser")
            products_links = soup.find_all("div", class_='inner_wrap TYPE_1')
            count_of_products += len(products_links)
        try:
            current_time = datetime.datetime.now(datetime.timezone.utc)
            insert_to_ranking_products(category_name, count_of_products, category_url, current_time)
            logger.info(f"Inserted data for category_name {category_name}, url - {category_url} "
                             f"count_of_products: {count_of_products} in process_multiple_pages_data")
        except psycopg2.Error as e:
            logger.error(f"Error inserting data in process_multiple_pages_data: {e}")
        logger.info("Data insertion completed in process_multiple_pages_data.")

    @staticmethod
    def process_single_page_data(url):
        base_category_url = url
        category_url = base_category_url
        response = requests.get(base_category_url)
        soup = BeautifulSoup(response.text, "html.parser")
        category_name = soup.find("div", class_='topic__heading').find("h1").get_text()
        count_of_products = len(soup.find_all("div", class_='inner_wrap TYPE_1'))
        try:
            current_time = datetime.datetime.now(datetime.timezone.utc)
            insert_to_ranking_products(category_name, count_of_products, category_url, current_time)
            logger.info(f"Inserted data for category_name {category_name}, url - {category_url} "
                             f"count_of_products: {count_of_products} in process_single_page_data")
        except psycopg2.Error as e:
            logger.error(f"Error inserting data in process_single_page_data: {e}")
        logger.info("Data insertion completed in process_single_page_data.")

    @classmethod
    def find_duplicates(cls):
        find_duplicates()

    @classmethod
    def remove_duplicates(cls):
        remove_duplicates()


obj = RankProds()
obj.get_products_count()
obj.find_duplicates()
obj.remove_duplicates()
