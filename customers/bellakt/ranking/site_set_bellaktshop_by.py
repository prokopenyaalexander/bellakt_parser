import datetime
import requests
from bs4 import BeautifulSoup
import logging
import os
from sqlalchemy import insert, text
from config.orm_core import engine
from config.paths_config import site_set
from config.headers import header
from config.config_queries import siteset_orm

date = datetime.date.today()
log_directory = site_set # ~/Documents/projects/profidata/customers/bellakt/logs/site_set_logs
os.makedirs(log_directory, exist_ok=True)
log_file_path = os.path.join(log_directory, f'site_set_{date}.log')

logger = logging.getLogger('SiteSetlogger')
logger.setLevel(logging.INFO)
handler = logging.FileHandler(log_file_path, mode='w')
handler.setFormatter(logging.Formatter(fmt='[%(asctime)s: %(levelname)s] %(message)s'))
logger.addHandler(handler)


class SiteSet:
    def __init__(self, url):
        self.url = url

    def get_categories(self):
        self.clear_table()
        logger.info('Work started')
        try:
            response = requests.get(self.url, headers=header)
            logger.info(f"URL {self.url} is available status_code - {response.status_code}")
            soup = BeautifulSoup(response.text, "html.parser")
            categories_tree = []
            links = soup.find_all("div", class_='item_block lg col-lg-20 col-md-4 col-xs-6')
            for link in links:
                category_name = link.find('span').get_text().strip()
                category_link = link.find('a').get('href')
                category_url = 'https://bellaktshop.by' + category_link
                main_category = f"Каталог *** {category_name}"  # Добавляем основную категорию
                categories_tree.append(main_category)
                logger.info(f'MAIN CATEGORY ADDED {main_category} URL - {category_url}')
                current_time = datetime.datetime.now(datetime.timezone.utc)
                self.insert_to_db_site_set(main_category, category_url, current_time)
                logger.info(f'Added record {main_category}, {category_url}')
                # Process all nesting levels
                self.process_category(category_url, categories_tree, main_category)
        except requests.exceptions.RequestException as e:
            logger.error(f'Error while query: {str(e)}')
        except Exception as e:
            logger.error(f'Error happend: {str(e)}')
        logger.info('End of processing.')

    def process_category(self, category_url, categories_tree, parent_category):
        logger.info(f"Processing category: {category_url}")
        try:
            response = requests.get(category_url, headers=header)
            soup = BeautifulSoup(response.text, "html.parser")
            sub_categories = soup.find_all('div', class_='col-lg-3 col-md-4 col-xs-6 col-xxs-12')

            for sub_category in sub_categories:
                sub_category_name = sub_category.find('span').get_text().strip()
                sub_category_link = sub_category.find('a').get('href')
                sub_category_url = 'https://bellaktshop.by' + sub_category_link
                category_name = f"{parent_category} *** {sub_category_name}"
                categories_tree.append(category_name)
                current_time = datetime.datetime.now(datetime.timezone.utc)
                self.insert_to_db_site_set(category_name, sub_category_url, current_time)
                logger.info(f'Added record {category_name}, {category_url}')
                # Recursively process subcategories
                subcategory_link = sub_category.find('a').get('href')
                subcategory_url = 'https://bellaktshop.by' + subcategory_link
                self.process_category(subcategory_url, categories_tree, category_name)

        except requests.exceptions.RequestException as e:
            logger.error(f"Error while requesting subcategories: {str(e)}")
        except Exception as e:
            logger.error(f"An error occurred while processing subcategories: {str(e)}")

    @staticmethod
    def insert_to_db_site_set(name, url, created_at):
        try:
            with engine.connect() as connection:
                # Создаем запрос на вставку
                insert_stmt = insert(siteset_orm).values(name=name, url=url, created_at=created_at)
                connection.execute(insert_stmt)
                connection.commit()
                logger.info("Success")
        except Exception as e:
            connection.rollback()
            logger.error(f"Error while inserting data: {str(e)}")

    @staticmethod
    def clear_table():
        try:
            with engine.connect() as connection:
                connection.execute(text("TRUNCATE TABLE siteset_orm RESTART IDENTITY CASCADE"))
                connection.commit()
                logger.info('Table siteset_orm cleared.')
        except Exception as e:
            logger.error(f"Error while clearing table: {str(e)}")

main_url = "https://bellaktshop.by/catalog"

obj = SiteSet(main_url)
obj.get_categories()
