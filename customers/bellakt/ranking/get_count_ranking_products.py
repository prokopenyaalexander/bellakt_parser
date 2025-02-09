import datetime
import logging
import os
import psycopg2
import requests
from bs4 import BeautifulSoup
from sqlalchemy import select, insert, func, delete
from config.config_queries import (ranking_products_orm, SessionLocal)
from config.models import SiteSet, RankingProducts
from config.orm_core import engine
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
        records = self.select_all_from_site_set()
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


    def process_multiple_pages_data(self,url, number_of_pages):
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
            self.insert_to_ranking_products(category_name, count_of_products, category_url, current_time)
            logger.info(f"Inserted data for category_name {category_name}, url - {category_url} "
                             f"count_of_products: {count_of_products} in process_multiple_pages_data")
        except psycopg2.Error as e:
            logger.error(f"Error inserting data in process_multiple_pages_data: {e}")
        logger.info("Data insertion completed in process_multiple_pages_data.")


    def process_single_page_data(self, url):
        base_category_url = url
        category_url = base_category_url
        response = requests.get(base_category_url)
        soup = BeautifulSoup(response.text, "html.parser")
        category_name = soup.find("div", class_='topic__heading').find("h1").get_text()
        count_of_products = len(soup.find_all("div", class_='inner_wrap TYPE_1'))
        try:
            current_time = datetime.datetime.now(datetime.timezone.utc)
            self.insert_to_ranking_products(category_name, count_of_products, category_url, current_time)
            logger.info(f"Inserted data for category_name {category_name}, url - {category_url} "
                             f"count_of_products: {count_of_products} in process_single_page_data")
        except psycopg2.Error as e:
            logger.error(f"Error inserting data in process_single_page_data: {e}")
        logger.info("Data insertion completed in process_single_page_data.")

    @staticmethod
    def select_all_from_site_set():
        try:
            with engine.connect() as connection:
                stmt = select(SiteSet.name, SiteSet.url)
                result = connection.execute(stmt)
                records = result.fetchall()
        except Exception as e:
            print(f"Error while executing SELECT query: {str(e)}")
        return records

    @staticmethod
    def insert_to_ranking_products(name, cnt_products, url, dt):
        try:
            with engine.connect() as connection:
                insert_stmt = insert(ranking_products_orm).values(
                    category_name=name,
                    count_of_products=cnt_products,
                    category_url=url,
                    date=dt
                )
                connection.execute(insert_stmt)
                connection.commit()
                logger.info(f'Data inserted: {name}, {cnt_products}, {url}')
        except Exception as e:
            connection.rollback()
            logger.error(f"Error while inserting data: {str(e)}")

    # @staticmethod
    # def find_duplicates():
    #     session = SessionLocal()
    #     try:
    #         # Создаем запрос для поиска дубликатов, используя функцию func для вызова PostgreSQL функции DATE
    #         duplicates_stmt = select(
    #             RankingProducts.category_url,
    #             func.date(RankingProducts.date).label('date'),  # Используем func.date для извлечения только даты
    #             func.count().label('count')
    #         ).group_by(
    #             RankingProducts.category_url,
    #             func.date(RankingProducts.date)  # Группируем по дате без времени
    #         ).having(func.count() > 1)
    #         # Выполняем запрос
    #         duplicates = session.execute(duplicates_stmt).fetchall()
    #
    #         if duplicates:
    #             for dup in duplicates:
    #                 print(
    #                     f"Category URL count_records_in_ranking_products: {dup.category_url}, Date: {dup.date}, Count: {dup.count}")
    #         else:
    #             print("No duplicates found.")
    #     finally:
    #         session.close()

    @staticmethod
    def remove_duplicates():
        session = SessionLocal()
        try:
            # Подзапрос для получения id дубликатов, группируя по дате без времени
            subquery = (
                select(
                    RankingProducts.id,
                    func.row_number().over(
                        partition_by=[RankingProducts.category_url, func.date(RankingProducts.date)]
                    ).label('rownum')
                )
                .subquery()
            )

            # Запрос на удаление дубликатов
            delete_stmt = delete(RankingProducts).where(
                RankingProducts.id.in_(
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

obj = RankProds()
obj.get_products_count()
obj.remove_duplicates()
