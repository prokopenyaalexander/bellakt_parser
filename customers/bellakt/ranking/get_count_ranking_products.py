import datetime
import logging
import os
import psycopg2
import requests
from bs4 import BeautifulSoup
from psycopg2 import OperationalError
from config.db_config import create_connection
from config.paths_config import ranking
from config.time_config import time_format

log_directory = ranking
os.makedirs(log_directory, exist_ok=True)
date = datetime.date.today()
log_file_path = os.path.join(log_directory, f'get_count_ranking_products_data_{date}.log')

logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    filemode='w',
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt=time_format
)


def get_products_count(connection):
    query = "SELECT name, url from site_tree where shop_id=101"
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        logging.info(f"Data Extracted from site_tree table. QUERY: {query} ")
        result = cursor.fetchall()
    except OperationalError as e:
        logging.error(f"Errorhas happend: {e}")
    logging.info(f"Starting processing data")

    for row in result:
        url = row[1]
        response = requests.get(url)
        soup = BeautifulSoup(response.text, "html.parser")
        if soup.find("div", class_="nums"):
            number_of_pages = int(soup.find("div", class_="nums").find_all("a", class_="dark_link" )[-1].text)
            process_multiple_pages_data(url, number_of_pages, connection)
        else:
            process_single_page_data(url, connection)


def process_multiple_pages_data(url, number_of_pages, connection):
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
        with connection.cursor() as cur:
            date_of_insertion = datetime.date.today()
            cur.execute(
                "SELECT COUNT(*) FROM ranking_products WHERE category_name = %s AND date::date = %s",
                (category_name, date_of_insertion)
            )
            exists = cur.fetchone()[0] > 0
            if exists:
                # Обновление существующей записи
                cur.execute(
                    "UPDATE ranking_products SET category_name = %s, date = %s, category_url = %s "
                    "WHERE category_name = %s AND date::date = %s",
                    (category_name, date_of_insertion, category_url, category_name, date_of_insertion)
                )
                logging.info(f"Updated data for category_name: {category_name}, url - {category_url} {date_of_insertion}")
            else:
                # Вставка новой записи
                cur.execute(
                    "INSERT INTO ranking_products (category_name, count_of_products, date, category_url)"
                    " VALUES (%s, %s, %s, %s)",
                    (category_name, count_of_products, date_of_insertion, category_url)
                )
                logging.info(f"Inserted data for category_name {category_name}, url - {category_url} "
                             f"count_of_products: {count_of_products} in process_multiple_pages_data")
            connection.commit()  # Выполняем коммит после успешной вставки
    except psycopg2.Error as e:
        logging.error(f"Error inserting data in process_multiple_pages_data: {e}")
        connection.rollback()
    logging.info("Data insertion completed in process_multiple_pages_data.")


def process_single_page_data(url, connection):
    base_category_url = url
    category_url = base_category_url
    response = requests.get(base_category_url)
    soup = BeautifulSoup(response.text, "html.parser")
    category_name = soup.find("div", class_='topic__heading').find("h1").get_text()
    count_of_products = len(soup.find_all("div", class_='inner_wrap TYPE_1'))
    try:
        with connection.cursor() as cur:
            date_of_insertion = datetime.date.today()
            cur.execute(
                "SELECT COUNT(*) FROM ranking_products WHERE category_name = %s AND date::date = %s ",
                (category_name, date_of_insertion)
            )
            exists = cur.fetchone()[0] > 0
            if exists:
                # Обновление существующей записи
                cur.execute(
                    "UPDATE ranking_products SET category_name = %s, date = %s, category_url = %s "
                    "WHERE category_name = %s AND date::date = %s",
                    (category_name, date_of_insertion, category_url, category_name, date_of_insertion)
                )
                logging.info(f"Updated data for category_name: {category_name}, url - {category_url} {date_of_insertion}")
            else:
                # Вставка новой записи
                cur.execute(
                    "INSERT INTO ranking_products (category_name, count_of_products, date, category_url) VALUES (%s, %s, %s, %s)",
                    (category_name, count_of_products, date_of_insertion, category_url)
                )
            logging.info(f"Inserted data for category_name {category_name}, url - {category_url} "
                         f"count_of_products: {count_of_products} in process_single_page_data")
            connection.commit()  # Выполняем коммит после успешной вставки
    except psycopg2.Error as e:
        logging.error(f"Error inserting data in process_single_page_data: {e}")
        connection.rollback()
    logging.info("Data insertion completed in process_single_page_data.")


connect = create_connection()
get_products_count(connect)
