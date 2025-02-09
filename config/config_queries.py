from sqlalchemy import insert, Table, MetaData, select, update, func, delete
from sqlalchemy.orm import sessionmaker
from config.models import SiteSet, RankingProducts, ProductContent
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

SessionLocal = sessionmaker(bind=engine)

# def select_all_from_ranking_products():
#     try:
#         with engine.connect() as connection:
#             stmt = select(RankingProducts.category_name, RankingProducts.date)
#             result = connection.execute(stmt)
#             records = result.fetchall()
#     except Exception as e:
#         print(f"Error while executing SELECT query: {str(e)}")
#     return records

# функция поиска дубликатов





