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
        logger.error(f"Error while clearing table: {str(e)}")

#  site_set block

def write_to_db_site_set(name, url, created_at):
    try:
        with engine.connect() as connection:
            # Создаем запрос на вставку
            insert_stmt = insert(siteset_orm).values(name=name, url=url, created_at=created_at)
            connection.execute(insert_stmt)
            connection.commit()
            logger.info(f'Data inserted: {name}, {url}, {created_at}')
    except Exception as e:
        connection.rollback()
        logger.error(f"Error while inserting data: {str(e)}")

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
        # Создаем запрос для поиска дубликатов
        duplicates_stmt = select(
            RankingProducts.category_url,
            RankingProducts.date,
            func.count().label('count')
        ).group_by(
            RankingProducts.category_url,
            RankingProducts.date
        ).having(func.count() > 1)

        # Выполняем запрос
        duplicates = session.execute(duplicates_stmt).fetchall()

        if duplicates:
            for dup in duplicates:
                print(f"Category URL: {dup.category_url}, Date: {dup.date}, Count: {dup.count}")
        else:
            print("No duplicates found.")

    except Exception as e:
        print(f"Error while finding duplicates: {str(e)}")
    finally:
        session.close()  # Закрываем сессию

# функция удаления дубликатов
def remove_duplicates():
    session = SessionLocal()
    try:
        # Подзапрос для получения id дубликатов
        subquery = (
            select(
                RankingProducts.id,
                func.row_number().over(partition_by=[RankingProducts.category_url, RankingProducts.date]).label('rownum')
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


# Pricing (add urls) block
def select_all_from_site_set_two():
    today = date.today()
    try:
        with engine.connect() as connection:
            stmt = select(SiteSet.name, SiteSet.url).where(
                and_(func.date(SiteSet.created_at) == today)
            )
            result = connection.execute(stmt)
            records = result.fetchall()
    except Exception as e:
        print(f"Error while executing SELECT query: {str(e)}")
    return records


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

# функция обновления записей
def udp_record_in_urls_to_crawling_orm(name, cnt_products, url, dt):
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
def count_records_in_urls_to_crawling_orm(category_name, date_of_insertion):
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
# SessionLocal = sessionmaker(bind=engine)
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

        if duplicates:
            for dup in duplicates:
                print(f"Category URL: {dup.pricing_url}, Date: {dup.date}, Count: {dup.count}")
        else:
            print("No duplicates found.")

    except Exception as e:
        print(f"Error while finding duplicates: {str(e)}")
    finally:
        session.close()  # Закрываем сессию

# функция удаления дубликатов
def remove_duplicates_urls_to_crawling_orm():
    session = SessionLocal()
    try:
        # Подзапрос для получения id дубликатов
        subquery = (
            select(
                UrlsToCrawling.id,
                func.row_number().over(partition_by=[UrlsToCrawling.pricing_url, UrlsToCrawling.date]).label('rownum')
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

        logging.info(f"Removed {result.rowcount} duplicate records.")

    except Exception as e:
        session.rollback()  # Откат транзакции в случае ошибки
        logging.error(f"Error while removing duplicates: {str(e)}")
    finally:
        session.close()  # Закрываем сессию


# Pricing  block
def select_all_from_urls_to_crawling_orm():
    try:
        with engine.connect() as connection:
            stmt = select(UrlsToCrawling.pricing_url)
            result = connection.execute(stmt)
            records = result.fetchall()
    except Exception as e:
        print(f"Error while executing SELECT query: {str(e)}")
    return records

def insert_to_urls_to_pricing_products_orm(sku, products_title, price, stock, url, date_of_insertion):
    try:
        with engine.connect() as connection:
            insert_stmt = insert(pricing_products_orm).values(
                sku=sku,
                name=products_title,
                price = price,
                stock = stock,
                product_url=url,
                date=date_of_insertion
            )
            connection.execute(insert_stmt)
            connection.commit()
            logger.info(f'Data inserted: {sku},  {products_title}')
    except Exception as e:
        connection.rollback()
        logger.error(f"Error while inserting data: {str(e)}")

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

        logging.info(f"Removed {result.rowcount} duplicate records.")

    except Exception as e:
        session.rollback()  # Откат транзакции в случае ошибки
        logging.error(f"Error while removing duplicates: {str(e)}")
    finally:
        session.close()  # Закрываем сессию

def find_duplicates_pricing_products_orm():
    session = SessionLocal()
    try:
        # Создаем запрос для поиска дубликатов
        duplicates_stmt = select(
            PricingProducts.sku,
            PricingProducts.date,
            func.count().label('count')
        ).group_by(
            PricingProducts.sku,
            PricingProducts.date
        ).having(func.count() > 1)

        # Выполняем запрос
        duplicates = session.execute(duplicates_stmt).fetchall()

        if duplicates:
            for dup in duplicates:
                print(f"Category URL: {dup.sku}, Date: {dup.date}, Count: {dup.count}")
        else:
            print("No duplicates found.")

    except Exception as e:
        print(f"Error while finding duplicates: {str(e)}")
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
                print(f"Category URL: {dup.sku}, Date: {dup.date}, Count: {dup.count}")
        else:
            print("No duplicates found.")

    except Exception as e:
        print(f"Error while finding duplicates: {str(e)}")
    finally:
        session.close()  # Закрываем сессию
