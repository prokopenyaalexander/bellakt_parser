import datetime
import logging
import requests
import os
from bs4 import BeautifulSoup
from sqlalchemy import select, and_, func, insert, delete

from config.config_queries import (remove_duplicates_urls_to_crawling_orm,
                                   find_duplicates_urls_to_crawling_orm, urls_to_crawling_orm, SessionLocal)
from config.models import SiteSet, UrlsToCrawling
from config.orm_core import engine
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
        records = self.select_all_from_site_set()
        for row in records:
            url = row[1]
            response = requests.get(url)
            soup = BeautifulSoup(response.text, "html.parser")
            if soup.find("div", class_="nums"):
                number_of_pages = int(soup.find("div", class_="nums").find_all("a", class_="dark_link")[-1].text)
                self.process_multiple_pages_data(url, number_of_pages)
            else:
                self.process_single_page_data(url)


    def process_multiple_pages_data(self, url, number_of_pages):
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
                    self.insert_to_urls_to_crawling_orm(url, category_name, date_of_insertion)
                    logger.info(f'url: {url} - added')
            except Exception as e:
                logger.error(f"Error inserting data in process_multiple_pages_data: {e}")
            logger.info("Data insertion completed in process_multiple_pages_data.")


    def process_single_page_data(self, url):
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
                self.insert_to_urls_to_crawling_orm(pricing_url, category_name, date_of_insertion)
                logger.info(f'url: {url} - added')
        else:
            logger.info(f"{pricing_urls}, EMPTY, {url}.")
        logger.info("Data insertion completed in process_single_page_data.")

    @staticmethod
    def select_all_from_site_set():
        today = date.today()
        try:
            with engine.connect() as connection:
                stmt = select(SiteSet.name, SiteSet.url).where(
                    and_(func.date(SiteSet.created_at) == today)
                )
                result = connection.execute(stmt)
                records = result.fetchall()
                logger.info(f"Records have been extracted")
        except Exception as e:
            logger.error(f"Error while executing SELECT query: {str(e)}")
        return records

    @staticmethod
    def insert_to_urls_to_crawling_orm(name, url, dt):
        try:
            with engine.connect() as connection:
                insert_stmt = insert(urls_to_crawling_orm).values(
                    pricing_url=name,
                    category_url=url,
                    date=dt
                )
                connection.execute(insert_stmt)
                connection.commit()
                logger.info(f'Data inserted: {name},  {url}')
        except Exception as e:
            connection.rollback()
            logger.error(f"Error while inserting data: {str(e)}")

    @staticmethod
    def find_duplicates_urls_to_crawling_orm():
        session = SessionLocal()
        try:
            # Создаем запрос для поиска дубликатов
            duplicates_stmt = select(
                UrlsToCrawling.pricing_url,
                UrlsToCrawling.date,
                func.count().label('count')
            ).group_by(
                UrlsToCrawling.pricing_url,
                UrlsToCrawling.date
            ).having(func.count() > 1)

            # Выполняем запрос
            duplicates = session.execute(duplicates_stmt).fetchall()

            # if duplicates:
            #     print(f"Duplicates found {duplicates}")
            # else:
            #     print("No duplicates found.")
            return duplicates

        except Exception as e:
            logger.error(f"Error while finding duplicates: {str(e)}")
        finally:
            session.close()  # Закрываем сессию

    @staticmethod
    def remove_duplicates_urls_to_crawling_orm():
        session = SessionLocal()
        try:
            # Подзапрос для получения id дубликатов
            subquery = (
                select(
                    UrlsToCrawling.id,
                    func.row_number().over(partition_by=[UrlsToCrawling.pricing_url, UrlsToCrawling.date]).label(
                        'rownum')
                )
                .subquery()
            )

            # Запрос на удаление дубликатов
            delete_stmt = delete(UrlsToCrawling).where(
                UrlsToCrawling.id.in_(
                    select(subquery.c.id).where(subquery.c.rownum > 1)
                )
            )

            # Выполнение запроса на удаление
            result = session.execute(delete_stmt)
            session.commit()  # Сохраняем изменения

            logger.info(f"Removed {result.rowcount} duplicate records.")

        except Exception as e:
            session.rollback()  # Откат транзакции в случае ошибки
            logger.error(f"Error while removing duplicates: {str(e)}")
        finally:
            session.close()  # Закрываем сессию

obj = UrlsToCrawl()
obj.get_pricing_urls()

