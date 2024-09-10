from sqlalchemy import text, insert, Table, MetaData
from config.orm_config import engine
import logging
import datetime
import os
from config.time_config import time_format
from config.paths_config import queries_log_directory


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


def write_to_db(name, url, created_at):
    try:
        with engine.connect() as connection:
            # Создаем запрос на вставку
            insert_stmt = insert(site_tree).values(name=name, url=url, created_at=created_at)
            connection.execute(insert_stmt)
            connection.commit()
            logger.info(f'Data inserted: {name}, {url}, {created_at}')
    except Exception as e:
        connection.rollback()
        logger.error(f"Error while inserting data: {str(e)}")
