import datetime
import logging
import requests
import os
from bs4 import BeautifulSoup
from config.config_queries import (insert_to_urls_to_crawling_orm, select_all_from_site_set_two,
                                   remove_duplicates_urls_to_crawling_orm, find_duplicates_urls_to_crawling_orm)
from config.paths_config import urls_to_pricing_module

log_directory = urls_to_pricing_module # ~/Documents/projects/profidata/customers/bellakt/logs/urls_to_pricing_module_logs
os.makedirs(log_directory, exist_ok=True)
date = datetime.date.today()
log_file_path = os.path.join(log_directory, f'urls_to_pricing_module_{date}.log')

logger = logging.getLogger('AddUrlslogger')
logger.setLevel(logging.INFO)
handler = logging.FileHandler(log_file_path, mode='w')
handler.setFormatter(logging.Formatter(fmt='[%(asctime)s: %(levelname)s] %(message)s'))
logger.addHandler(handler)

class UrlsToCrawl:

    def get_pricing_urls(self):
        records = select_all_from_site_set_two()
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
        page_param = f'?PAGEN_1='
        response = requests.get(base_category_url)
        soup = BeautifulSoup(response.text, "html.parser")
        category_name = soup.find("div", class_='topic__heading').find("h1").get_text()
        urls_to_crawl = [f'{url}{page_param}{gen_url}' for gen_url in range(2, number_of_pages + 1)]
        urls_to_crawl.insert(0, url)
        pricing_urls = []
        for url in urls_to_crawl:
            response = requests.get(url)
            soup = BeautifulSoup(response.text, "html.parser")
            products_links = soup.find_all("div", class_='inner_wrap TYPE_1')
            for link in products_links:

                product_link = 'https://bellaktshop.by' + link.find('a').get('href')
                pricing_urls.append(product_link)

        if pricing_urls:
            date_of_insertion = datetime.date.today()
            try:
                for url in pricing_urls:
                    insert_to_urls_to_crawling_orm(url, category_name, date_of_insertion)
                    logger.info(f'url: {url} - added')
            except Exception as e:
                logger.error(f"Error inserting data in process_multiple_pages_data: {e}")
            logger.info("Data insertion completed in process_multiple_pages_data.")


    @staticmethod
    def process_single_page_data(url):
        base_category_url = url
        response = requests.get(base_category_url)
        soup = BeautifulSoup(response.text, "html.parser")
        category_name = soup.find("div", class_='topic__heading').find("h1").get_text()
        products_links = soup.find_all("div", class_='inner_wrap TYPE_1')
        pricing_urls = []
        for link in products_links:
            product_link = 'https://bellaktshop.by' + link.find('a').get('href')
            pricing_urls.append(product_link)
            logger.info(f'Product link: {product_link} - added| BASEURL {base_category_url}')

        if pricing_urls:
            date_of_insertion = datetime.date.today()
            for url in pricing_urls:
                pricing_url = url
                insert_to_urls_to_crawling_orm(pricing_url, category_name, date_of_insertion)
                logger.info(f'url: {url} - added')
        else:
            logger.info(f"{pricing_urls}, EMPTY, {url}.")
        logger.info("Data insertion completed in process_single_page_data.")

    @staticmethod
    def find_duplicates():
        find_duplicates_urls_to_crawling_orm()

    @staticmethod
    def remove_duplicates():
        remove_duplicates_urls_to_crawling_orm()

obj = UrlsToCrawl()
obj.get_pricing_urls()
obj.find_duplicates()
obj.remove_duplicates()