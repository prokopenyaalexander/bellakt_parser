import datetime
import requests
from bs4 import BeautifulSoup
import logging
import os
from config.paths_config import site_set
from config.headers import header
from config.time_config import time_format
from config.config_queries import  write_to_db_site_set, clear_table


date = datetime.date.today()
log_directory = site_set
os.makedirs(log_directory, exist_ok=True)
log_file_path = os.path.join(log_directory, f'site_set_{date}.log')

logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    filemode='w',
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt=time_format
)


class SiteSet:
    def __init__(self, url):
        self.url = url

    def get_categories(self):
        clear_table()
        logging.info('Starting work')
        try:
            response = requests.get(self.url, headers=header)
            logging.info(f"URL {self.url} is available status_code - {response.status_code}")
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            categories_tree = []
            links = soup.find_all("div", class_='item_block lg col-lg-20 col-md-4 col-xs-6')
            for link in links:
                category_name = link.find('span').get_text().strip()
                category_link = link.find('a').get('href')
                category_url = 'https://bellaktshop.by' + category_link

                main_category = f"Каталог *** {category_name}"  # Добавляем основную категорию
                categories_tree.append(main_category)
                logging.info(f'MAIN CATEGORY ADDED {main_category} URL - {category_url}')
                current_time = datetime.datetime.now(datetime.timezone.utc)

                write_to_db_site_set(main_category, category_url, current_time)

                logging.info(f'Added record {main_category}, {category_url}')

                # Process all nesting levels
                self.process_category(category_url, categories_tree, main_category)
        except requests.exceptions.RequestException as e:
            logging.error(f'Error while query: {str(e)}')
        except Exception as e:
            logging.error(f'Error happend: {str(e)}')
        logging.info('End of processing.')

    def process_category(self, category_url, categories_tree, parent_category):
        logging.info(f"Processing category: {category_url}")
        try:
            response = requests.get(category_url, headers=header)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            sub_categories = soup.find_all('div', class_='col-lg-3 col-md-4 col-xs-6 col-xxs-12')

            for sub_category in sub_categories:
                sub_category_name = sub_category.find('span').get_text().strip()
                sub_category_link = sub_category.find('a').get('href')
                sub_category_url = 'https://bellaktshop.by' + sub_category_link
                category_name = f"{parent_category} *** {sub_category_name}"
                categories_tree.append(category_name)
                current_time = datetime.datetime.now(datetime.timezone.utc)

                write_to_db_site_set(category_name, sub_category_url, current_time)

                logging.info(f'Added record {category_name}, {category_url}')

                # Recursively process subcategories
                subcategory_link = sub_category.find('a').get('href')
                subcategory_url = 'https://bellaktshop.by' + subcategory_link
                self.process_category(subcategory_url, categories_tree, category_name)

        except requests.exceptions.RequestException as e:
            logging.error(f"Error while requesting subcategories: {str(e)}")
        except Exception as e:
            logging.error(f"An error occurred while processing subcategories: {str(e)}")


main_url = "https://bellaktshop.by/catalog"

obj = SiteSet(main_url)
obj.get_categories()
