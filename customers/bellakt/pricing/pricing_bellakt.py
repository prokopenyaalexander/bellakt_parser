import logging
import datetime
import os
import re
import psycopg2
import requests
from bs4 import BeautifulSoup
from psycopg2 import OperationalError
from config.db_config import create_connection
from config.paths_config import pricing_log_directory


time_format = '%Y-%m-%d %H:%M:%S'
log_directory = pricing_log_directory
os.makedirs(log_directory, exist_ok=True)
date = datetime.date.today()
log_file_path = os.path.join(log_directory, f'pricing_data_{date}.log')

logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    filemode='w',
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt=time_format
)


def get_pricing_details(connection):

    query = "SELECT pricing_url from urls_to_crawling"
    result = None
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
    except OperationalError as e:
        logging.error(f"The error occurred: {e}")

    for row in result:
        logging.info(f"Working with {row[0]} category.")
        try:
            response = requests.get(row[0])
            response.raise_for_status()
        except requests.RequestException as e:
            logging.error(f"Error fetching URL {row[0]}: {e}")
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
        writ_to_db(connection, sku, products_title, price, stock, url)


def writ_to_db(connection, sku, products_title, price, stock, url):
    try:
        with connection.cursor() as cur:
            date_of_insertion = datetime.date.today()
            # Проверка существующих записей
            cur.execute(
                "SELECT COUNT(*) FROM pricing_products WHERE sku = %s AND date::date = %s",
                (sku, date_of_insertion)
            )
            exists = cur.fetchone()[0] > 0

            if exists:
                # Обновление существующей записи
                cur.execute(
                    "UPDATE pricing_products SET sku = %s, name = %s, price = %s, stock = %s,"
                    "product_url = %s, date = %s WHERE sku = %s AND date::date = %s",
                    (sku, products_title, price, stock, url, date_of_insertion, sku, date_of_insertion)
                )
                logging.info(f"Updated data for SKU: {sku}")
            else:
                # Вставка новой записи
                cur.execute(
                    "INSERT INTO pricing_products (sku, name, price, stock, product_url, date)"
                    " VALUES (%s, %s, %s, %s, %s, %s)",
                    (sku, products_title, price, stock, url, date_of_insertion)
                )
                logging.info(f"Inserted data for SKU: {sku}")

            connection.commit()  # Выполняем коммит после успешной вставки
    except psycopg2.Error as e:
        logging.error(f"Error inserting data: {e}")
        connection.rollback()
    logging.info("Data insertion completed.")


connect = create_connection()

try:
    get_pricing_details(connect)
finally:
    connect.close()

