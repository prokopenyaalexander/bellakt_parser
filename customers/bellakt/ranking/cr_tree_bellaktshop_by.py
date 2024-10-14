import os
import requests
from bs4 import BeautifulSoup
import json
import logging
import datetime
from config.paths_config import cr_tree
from config.headers import header
from config.time_config import time_format

date = datetime.date.today()
log_directory = cr_tree
os.makedirs(log_directory, exist_ok=True)
log_file_path = os.path.join(log_directory, f'cr_tree_{date}.log')

logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    filemode='w',
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt=time_format
)


class GetCRTree:

    def __init__(self, url):
        self.url = url

    def get_cr_tree_categories(self):
        response = requests.get(self.url, headers=header)
        if response.status_code == 200:
            logging.info(f"URL {self.url} is available status_code - {response.status_code}")
            soup = BeautifulSoup(response.text, "html.parser")
            categories_tree = []
            links = soup.find_all("div", class_='item_block lg col-lg-20 col-md-4 col-xs-6')
            for link in links:
                category_name = link.find('span').get_text()
                category_link = link.find('a').get('href')
                category_url = 'https://bellaktshop.by' + category_link
                logging.info(f"Current category {category_name}, URL - {category_url} added to cr_tree")
                subcategories = self.process_cr_tree_category(category_url)
                categories_tree.append({
                    "name": category_name,
                    "url": 'https://bellaktshop.by' + category_link,
                    "subcategories": subcategories
                })
            output_directory = os.path.expanduser("~/Documents/projects/profidata/customers/bellakt/ranking/jsons/")
            os.makedirs(output_directory, exist_ok=True)
            file_path = os.path.join(output_directory, f"{datetime.date.today()} - categories_bellaktshop.json")
            with open(file_path, "w") as file:
                json.dump(categories_tree, file, indent=4)
                logging.info(f"Categories saved to categories_bellaktshop.json | {file_path}")
            return file_path
        else:
            logging.error(f"URL {self.url} is unavailable {response.status_code}")
        return True

    def process_cr_tree_category(self, category_url, visited_categories=None):
        if visited_categories is None:
            visited_categories = set()
        logging.info(f"Processing category: {category_url} ")
        response = requests.get(category_url, headers=header)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")
            links = soup.find_all('div', class_='col-lg-3 col-md-4 col-xs-6 col-xxs-12')
            subcategories = []
            for link in links:
                subcategory_name_tag = link.find('span')
                if subcategory_name_tag:
                    subcategory_name = subcategory_name_tag.get_text()
                    subcategory_link_tag = link.find('a')
                    if subcategory_link_tag:
                        subcategory_link = subcategory_link_tag.get('href')
                        subcategory_url = 'https://bellaktshop.by' + subcategory_link
                        if subcategory_url not in visited_categories:
                            visited_categories.add(subcategory_url)
                            subcategory = {
                                "name": subcategory_name,
                                "url": subcategory_url,
                                "subcategories": self.process_cr_tree_category(subcategory_url, visited_categories)
                            }
                            subcategories.append(subcategory)
                            logging.info(f"Added subcategory: {subcategory_name}")
            return subcategories
        else:
            logging.error(f"URL {category_url} is unavailable {response.status_code}")


# main_url = "https://bellaktshop.by/catalog"
#
# obj = GetCRTree(main_url)
# obj.get_cr_tree_categories()
