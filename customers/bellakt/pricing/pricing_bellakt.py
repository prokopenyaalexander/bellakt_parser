import logging
import datetime
import os
import re
import requests
from bs4 import BeautifulSoup
from sqlalchemy import insert, select, and_, func, delete
from sqlalchemy.orm import sessionmaker
from config.config_queries import pricing_products_orm, SessionLocal
from config.models import UrlsToCrawling, PricingProducts
from config.orm_core import engine
from config.paths_config import pricing_log_directory


log_directory = pricing_log_directory
os.makedirs(log_directory, exist_ok=True)
date = datetime.date.today()
log_file_path = os.path.join(log_directory, f'pricing_data_{date}.log')

logger = logging.getLogger('Pricinglogger')
logger.setLevel(logging.INFO)
handler = logging.FileHandler(log_file_path, mode='w')
handler.setFormatter(logging.Formatter(fmt='[%(asctime)s: %(levelname)s] %(message)s'))
logger.addHandler(handler)

Session = sessionmaker(bind=engine)


class Pricing:

    def get_pricing_details(self):
        records = self.select_all_from_urls_to_crawling_orm()
        for row in records:
            logger.info(f"Working with {row[0]} url of category.")
            try:
                response = requests.get(row[0], timeout=5)
                response.raise_for_status()
            except requests.RequestException as e:
                logger.error(f"Error fetching URL {row[0]}: {e}")
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
            date_of_insertion = datetime.datetime.now(datetime.timezone.utc)
            self.insert_to_urls_to_pricing_products_orm(sku, products_title, price, stock, url, date_of_insertion)
        logger.info(f"Finished work")

    @staticmethod
    def select_all_from_urls_to_crawling_orm():
        try:
            with engine.connect() as connection:
                today = date.today()

                stmt = select(UrlsToCrawling.pricing_url).where(
                    func.date(UrlsToCrawling.date) == today
                )
                result = connection.execute(stmt)
                records = result.fetchall()
                logger.info(f"Records have been extracted")
        except Exception as e:
            logger.error(f"Error while executing SELECT query: {str(e)}")
        return records

    @staticmethod
    def insert_to_urls_to_pricing_products_orm(sku, products_title, price, stock, url, date_of_insertion):
        try:
            with engine.connect() as connection:
                insert_stmt = insert(pricing_products_orm).values(
                    sku=sku,
                    name=products_title,
                    price=price,
                    stock=stock,
                    product_url=url,
                    date=date_of_insertion
                )
                connection.execute(insert_stmt)
                connection.commit()
                logger.info(f'Data inserted: {sku},  {products_title}, {url}')
        except Exception as e:
            connection.rollback()
            logger.error(f"Error while inserting data: {str(e)}")

    @staticmethod
    def remove_duplicates_pricing_products_orm():
        session = SessionLocal()
        try:
            # Подзапрос для получения id дубликатов
            subquery = (
                select(
                    PricingProducts.id,
                    func.row_number().over(partition_by=[PricingProducts.sku, PricingProducts.date]).label('rownum')
                )
                .subquery()
            )

            # Запрос на удаление дубликатов
            delete_stmt = delete(PricingProducts).where(
                PricingProducts.id.in_(
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


obj = Pricing()
obj.get_pricing_details()

