import json
import logging
import datetime
import os
import re
import requests
from bs4 import BeautifulSoup
from sqlalchemy import select, and_, func, delete, insert
from config.config_queries import SessionLocal, product_content_orm
from config.models import UrlsToCrawling, ProductContent
from config.orm_core import engine
from config.paths_config import pc_log_directory

log_directory = pc_log_directory # ~/Documents/projects/profidata/customers/bellakt/logs/pc_logs
os.makedirs(log_directory, exist_ok=True)
date = datetime.date.today()
log_file_path = os.path.join(log_directory, f'pc_data_{date}.log')


logger = logging.getLogger('PClogger')
logger.setLevel(logging.INFO)
handler = logging.FileHandler(log_file_path, mode='w')
handler.setFormatter(logging.Formatter(fmt='[%(asctime)s: %(levelname)s] %(message)s'))
logger.addHandler(handler)

class PC:


    def get_pc_details(self):
        records = self.select_all_from_urls_to_crawling_orm()
        logger.info("Records have been extracted")
        print(len(records))
        best_before_date = None  # срок годности
        for row in records:
            try:
                response = requests.get(row[0])
                response.raise_for_status()
            except requests.RequestException as e:
                logger.error(f"Error fetching URL {row[0]}: {e}")
                continue
            soup = BeautifulSoup(response.text, "html.parser")
            pattern = r'\b\d{3,}\b'
            sku = re.findall(pattern, row[0])[0]  # находим id продукта
            products_title = soup.find("div", class_="topic__heading").find("h1").get_text()  # находим имя продукта
            number_of_images = len([soup.find("div", class_="product-detail-gallery__item product-detail-gallery__item--"
                                                            "middle text-center").find("a").get("href")])  # находим кол-во картинок продукта

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
            date_of_insertion = datetime.date.today()
            self.insert_to_urls_to_product_content_orm(sku, products_title, number_of_images, best_before_date,
                                                  json.dumps(data), date_of_insertion)
        logger.info("Data insertion completed.")
        if self.find_duplicates_product_content_orm():
            self.remove_duplicates_product_content_orm()
            logger.info(f"Duplicates found")

    @staticmethod
    def select_all_from_urls_to_crawling_orm():
        try:
            with engine.connect() as connection:
                # stmt = select(UrlsToCrawling.pricing_url)
                today = date.today()
                stmt = select(UrlsToCrawling.pricing_url).where(
                    and_(func.date(UrlsToCrawling.date) == func.current_date())
                )
                result = connection.execute(stmt)
                records = result.fetchall()
        except Exception as e:
            logger.error(f"Error while executing SELECT query: {str(e)}")
        return records

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
                    func.row_number().over(partition_by=[ProductContent.sku, ProductContent.date]).label('rownum')
                )
                .subquery()
            )

            # Запрос на удаление дубликатов
            delete_stmt = delete(ProductContent).where(
                ProductContent.id.in_(
                    select(subquery.c.id).where(subquery.c.rownum > 1)
                )
            )

            # Выполнение запроса на удаление
            result = session.execute(delete_stmt)
            session.commit()  # Сохраняем изменения

            logging.info(f"Removed {result.rowcount} duplicate records.")

        except Exception as e:
            session.rollback()  # Откат транзакции в случае ошибки
            logger.error(f"Error while removing duplicates: {str(e)}")
        finally:
            session.close()  # Закрываем сессию

obj=PC()
obj.get_pc_details()

