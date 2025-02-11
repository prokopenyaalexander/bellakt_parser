import logging
import datetime
import os
import re
import requests
from bs4 import BeautifulSoup
from sqlalchemy import insert, select,func, delete
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from config.config_queries import pricing_products_orm, SessionLocal, product_content_orm
from config.models import UrlsToCrawling, PricingProducts, ProductContent
from config.orm_core import engine
from config.paths_config import pricing_log_directory
import json


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
            ####
            number_of_images = len([soup.find("div", class_="product-detail-gallery__item product-detail-"
                                                            "gallery__item--middle text-center").find("a").get("href")])  # находим кол-во картинок продукта
            best_before_date = None
            title_best_before_date_element = soup.find('p', class_='title', string='Срок годности')  # срок годности
            if title_best_before_date_element:
                info_block = title_best_before_date_element.find_next_sibling('div', class_='info-block')
                if info_block:
                    best_before_date = info_block.get_text(strip=True)
                else:
                    logger.error(f"Информация о сроке годности не найдена: {row[0]}")
            else:
                logger.error(f"Элемент с заголовком 'Срок годности' не найден: {row[0]}")
                best_before_date = "Не найден"

            data = {}
            characteristic = (soup.find("div", class_="tab-content").find("table", class_="props_list nbg")
                              .find_all('tr', itemprop='additionalProperty'))
            for line in characteristic:
                name = line.find('td', class_='char_name').find('span', itemprop='name').get_text(strip=True)
                value = line.find('td', class_='char_value').find('span', itemprop='value').get_text(strip=True)
                data[name] = value



            ###
            if temp_stock == "В корзину":
                stock = "In stock"
            else:
                stock = "OOS"
            url = row[0]  # потому что, нужно использовать параметризацию.
            date_of_insertion = datetime.datetime.now(datetime.timezone.utc)

            self.insert_to_urls_to_pricing_products_orm(sku, products_title, price, stock, url, date_of_insertion)
            self.insert_to_urls_to_product_content_orm(sku, products_title, number_of_images, best_before_date,
                                                       json.dumps(data), date_of_insertion)

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
                    func.row_number().over(
                        partition_by=[PricingProducts.sku, func.date(PricingProducts.date)],
                        order_by=PricingProducts.id  # Определяем порядок для ROW_NUMBER()
                    ).label('rownum')
                )
                .filter(PricingProducts.sku.isnot(None))  # Исключаем записи с NULL в sku
                .filter(PricingProducts.date.isnot(None))  # Исключаем записи с NULL в date
                .subquery()
            )

            # Выводим результат подзапроса для отладки
            debug_query = select(subquery.c.id, subquery.c.rownum)
            debug_results = session.execute(debug_query).all()
            logger.info(f"Debugging subquery results: {debug_results}")

            if not debug_results:
                logger.info("No duplicates found.")
                return

            # Запрос на удаление дубликатов
            delete_stmt = (
                delete(PricingProducts)
                .where(
                    PricingProducts.id.in_(
                        select(subquery.c.id).where(subquery.c.rownum > 1)  # Удаляем записи с rownum > 1
                    )
                )
            )

            # Выполнение запроса на удаление
            result = session.execute(delete_stmt)
            session.commit()  # Сохраняем изменения

            logger.info(f"Removed {result.rowcount} duplicate records.")
        except SQLAlchemyError as e:
            session.rollback()  # Откат транзакции в случае ошибки
            logger.error(f"Database error while removing duplicates: {str(e)}")
        except Exception as e:
            session.rollback()  # Откат транзакции в случае других ошибок
            logger.error(f"Unexpected error while removing duplicates: {str(e)}")
        finally:
            session.close()  # Закрываем сессию

#################################
    @staticmethod
    def insert_to_urls_to_product_content_orm(sku, title, number_of_images, best_before_date, characteristic,
                                              date_of_insertion):
        try:
            with engine.connect() as connection:
                insert_stmt = insert(product_content_orm).values(
                    sku=sku,
                    title=title,
                    number_of_images=number_of_images,
                    best_before_date=best_before_date,
                    characteristic=characteristic,
                    date=date_of_insertion
                )
                connection.execute(insert_stmt)
                connection.commit()
                logger.info(f'Data inserted: {sku},  {title}')
        except Exception as e:
            connection.rollback()
            logger.error(f"Error while inserting data: {str(e)}")

    @staticmethod
    def find_duplicates_product_content_orm():
        session = SessionLocal()
        try:
            # Создаем запрос для поиска дубликатов
            duplicates_stmt = select(
                ProductContent.sku,
                ProductContent.date,
                func.count().label('count')
            ).group_by(
                ProductContent.sku,
                ProductContent.date
            ).having(func.count() > 1)

            # Выполняем запрос
            duplicates = session.execute(duplicates_stmt).fetchall()

            return duplicates

        except Exception as e:
            logger.error(f"Error while finding duplicates: {str(e)}")
        finally:
            session.close()  # Закрываем сессию

    @staticmethod
    def remove_duplicates_product_content_orm():
        session = SessionLocal()
        try:
            # Подзапрос для получения id дубликатов
            subquery = (
                select(
                    ProductContent.id,
                    func.row_number().over(
                        partition_by=[ProductContent.sku, func.date(ProductContent.date)],  # Группировка по sku и дате
                        order_by=ProductContent.id  # Определяем порядок для ROW_NUMBER()
                    ).label('rownum')
                )
                .filter(ProductContent.sku.isnot(None))  # Исключаем записи с NULL в sku
                .filter(ProductContent.date.isnot(None))  # Исключаем записи с NULL в date
                .subquery()
            )

            # Запрос на удаление дубликатов
            delete_stmt = (
                delete(ProductContent)
                .where(
                    ProductContent.id.in_(
                        select(subquery.c.id).where(subquery.c.rownum > 1)  # Удаляем записи с rownum > 1
                    )
                )
            )

            # Выполнение запроса на удаление
            result = session.execute(delete_stmt)
            session.commit()  # Сохраняем изменения

            logging.info(f"Removed {result.rowcount} duplicate records. (remove_duplicates_product_content_orm method)")
        except SQLAlchemyError as e:
            session.rollback()  # Откат транзакции в случае ошибки
            logger.error(f"Database error while removing duplicates: {str(e)}")
        except Exception as e:
            session.rollback()  # Откат транзакции в случае других ошибок
            logger.error(f"Unexpected error while removing duplicates: {str(e)}")
        finally:
            session.close()  # Закрываем сессию




obj = Pricing()
obj.get_pricing_details()
obj.remove_duplicates_pricing_products_orm()
obj.remove_duplicates_product_content_orm()
