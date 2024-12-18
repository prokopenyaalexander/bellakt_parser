from sqlalchemy import text, insert, Table, MetaData, select, update, func, delete, and_
from sqlalchemy.orm import sessionmaker, Session
from config.models import SiteSet, RankingProducts, UrlsToCrawling, ProductContent, PricingProducts
from config.orm_core import engine
import logging
import datetime
import os
from config.time_config import time_format
from config.paths_config import queries_log_directory
# from orm_core import create_connection


date = datetime.date.today()
log_directory = queries_log_directory
os.makedirs(log_directory, exist_ok=True)
log_file_path = os.path.join(log_directory, f'queries-{date}.log')

logger = logging.getLogger('dbQueriesLogger')
logger.setLevel(logging.INFO)
handler = logging.FileHandler(log_file_path, mode='w')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt=time_format)
handler.setFormatter(formatter)
logger.addHandler(handler)


metadata = MetaData()
site_tree = Table('site_tree', metadata, autoload_with=engine)
siteset_orm = Table('siteset_orm', metadata, autoload_with=engine)
ranking_products_orm = Table('ranking_products_orm', metadata, autoload_with=engine)
urls_to_crawling_orm = Table('urls_to_crawling_orm', metadata, autoload_with=engine)
pricing_products_orm = Table('pricing_products_orm', metadata, autoload_with=engine)
product_content_orm = Table('product_content_orm', metadata, autoload_with=engine)

#  common block
def clear_table():
    try:
        with engine.connect() as connection:
            connection.execute(text("TRUNCATE TABLE siteset_orm RESTART IDENTITY CASCADE"))
            connection.commit()
            logger.info('Table siteset_orm cleared.')
    except Exception as e:
        print(f"Error while clearing table: {str(e)}")

#  site_set block
def insert_to_db_site_set(name, url, created_at):
    try:
        with engine.connect() as connection:
            # Создаем запрос на вставку
            insert_stmt = insert(siteset_orm).values(name=name, url=url, created_at=created_at)
            connection.execute(insert_stmt)
            connection.commit()
    except Exception as e:
        connection.rollback()
        print(f"Error while inserting data: {str(e)}")

#  get_count_ranking_products block
def select_all_from_site_set():
    try:
        with engine.connect() as connection:
            stmt = select(SiteSet.name, SiteSet.url)
            result = connection.execute(stmt)
            records = result.fetchall()
    except Exception as e:
        print(f"Error while executing SELECT query: {str(e)}")
    return records

def select_all_from_ranking_products():
    try:
        with engine.connect() as connection:
            stmt = select(RankingProducts.category_name, RankingProducts.date)
            result = connection.execute(stmt)
            records = result.fetchall()
    except Exception as e:
        print(f"Error while executing SELECT query: {str(e)}")
    return records

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

# функция обновления записей
def udp_record_in_ranking_products(name, cnt_products, url, dt):
    try:
        with (engine.connect() as connection):
            update_stmt = (
            update(RankingProducts)
            .where(
                (RankingProducts.category_name == name) &
                (RankingProducts.date == dt)
            )
            .values(
                category_name=name,
                count_of_products=cnt_products,
                category_url=url,
                date=dt
                )
            )
            connection.execute(update_stmt)
            connection.commit()
            logger.info(f'Data updated: {name}, {cnt_products}, {url}')
    except Exception as e:
        connection.rollback()
        logger.error(f"Error while inserting data: {str(e)}")

# функция удаления дубликатов
def count_records_in_ranking_products(category_name, date_of_insertion):
    try:
        with engine.connect() as connection:
            # Создаем запрос для подсчета записей
            stmt = select(func.count()).filter(
                (RankingProducts.category_name == category_name) &
                (RankingProducts.date == date_of_insertion)
            )
            result = connection.execute(stmt)

    except Exception as e:
        print(f"Error while executing SELECT COUNT query: {str(e)}")
        return  result

# функция поиска дубликатов
SessionLocal = sessionmaker(bind=engine)
def find_duplicates():
    session = SessionLocal()
    try:
        # Создаем запрос для поиска дубликатов, используя функцию func для вызова PostgreSQL функции DATE
        duplicates_stmt = select(
            RankingProducts.category_url,
            func.date(RankingProducts.date).label('date'),  # Используем func.date для извлечения только даты
            func.count().label('count')
        ).group_by(
            RankingProducts.category_url,
            func.date(RankingProducts.date)  # Группируем по дате без времени
        ).having(func.count() > 1)
        # Выполняем запрос
        duplicates = session.execute(duplicates_stmt).fetchall()

        if duplicates:
            for dup in duplicates:
                print(f"Category URL count_records_in_ranking_products: {dup.category_url}, Date: {dup.date}, Count: {dup.count}")
        else:
            print("No duplicates found.")
    finally:
        session.close()

# функция удаления дубликатов
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

        logging.info(f"Removed {result.rowcount} duplicate records.")

    except Exception as e:
        session.rollback()  # Откат транзакции в случае ошибки
        logging.error(f"Error while removing duplicates: {str(e)}")
    finally:
        session.close()  # Закрываем сессию

# PC block
def insert_to_urls_to_product_content_orm(sku, title, number_of_images, best_before_date, characteristic, date_of_insertion):
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
        logging.error(f"Error while removing duplicates: {str(e)}")
    finally:
        session.close()  # Закрываем сессию

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

        if duplicates:
            for dup in duplicates:
                print(f"Category URL find_duplicates_product_content_orm: {dup.sku}, Date: {dup.date}, Count: {dup.count}")
        else:
            print("No duplicates found.")

    except Exception as e:
        print(f"Error while finding duplicates: {str(e)}")
    finally:
        session.close()  # Закрываем сессию
