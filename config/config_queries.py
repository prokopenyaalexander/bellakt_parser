from sqlalchemy import text, insert, Table, MetaData, select, update, func, delete
from sqlalchemy.orm import sessionmaker, Session
from config.models import SiteSet, RankingProducts
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

#  common block


def clear_table():
    try:
        with engine.connect() as connection:
            connection.execute(text("TRUNCATE TABLE site_tree RESTART IDENTITY CASCADE"))
            connection.commit()
            logger.info('Table site_tree cleared.')
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


# Вызов функции для поиска дубликатов
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



