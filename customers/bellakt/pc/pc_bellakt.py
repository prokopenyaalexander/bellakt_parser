import logging
import datetime
import os
import re
import psycopg2
import json
import requests
from bs4 import BeautifulSoup
from psycopg2 import OperationalError
from config.db_config import create_connection
from config.paths_config import pc_log_directory
from config.time_config import time_format

log_directory = pc_log_directory
os.makedirs(log_directory, exist_ok=True)
date = datetime.date.today()
log_file_path = os.path.join(log_directory, f'pc_data_{date}.log')


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
        logging.error(f"Error fetching URLs: {e}")

    best_before_date = None  # срок годности
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
        sku = re.findall(pattern, row[0])[0]  # находим id продукта
        products_title = soup.find("div", class_="topic__heading").find("h1").get_text()  # находим имя продукта
        number_of_images = len([soup.find("div", class_="product-detail-gallery__item product-detail-gallery__item--"
                                                        "middle text-center").find("a").get("href")])  # находим кол-во
        # картинок продукта

        title_best_before_date_element = soup.find('p', class_='title', string='Срок годности')  # срок годности
        if title_best_before_date_element:
            info_block = title_best_before_date_element.find_next_sibling('div', class_='info-block')
            if info_block:
                best_before_date = info_block.get_text(strip=True)
            else:
                logging.error(f"Информация о сроке годности не найдена: {row[0]}")
                # best_before_date = "Не найден срок годности"
        else:
            logging.error(f"Элемент с заголовком 'Срок годности' не найден: {row[0]}")
            best_before_date = "Не найден"

        data = {}
        characteristic = (soup.find("div", class_="tab-content").find("table", class_="props_list nbg")
                          .find_all('tr', itemprop='additionalProperty'))
        for line in characteristic:
            name = line.find('td', class_='char_name').find('span', itemprop='name').get_text(strip=True)
            value = line.find('td', class_='char_value').find('span', itemprop='value').get_text(strip=True)
            data[name] = value
        try:
            with connection.cursor() as cur:
                date_of_insertion = datetime.date.today()
                # Проверка существующих записей
                cur.execute(
                    "SELECT COUNT(*) FROM product_content WHERE sku = %s AND date::date = %s",
                    (sku, date_of_insertion)
                )
                exists = cur.fetchone()[0] > 0

                if exists:
                    # Обновление существующей записи
                    cur.execute(
                        "UPDATE product_content SET title = %s, number_of_images = %s, best_before_date = %s, "
                        "characteristic = %s, date = %s WHERE sku = %s AND date::date = %s",
                        (products_title, number_of_images, best_before_date, json.dumps(data), date_of_insertion, sku,
                         date_of_insertion)
                    )
                    logging.info(f"Updated data for SKU: {sku}")
                else:
                    # Вставка новой записи
                    cur.execute(
                        "INSERT INTO product_content (sku, title, number_of_images, best_before_date, characteristic, "
                        "date) VALUES (%s, %s, %s, %s, %s, %s)",
                        (sku, products_title, number_of_images, best_before_date, json.dumps(data), date_of_insertion)
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
