import datetime
import logging
import requests
import os
from bs4 import BeautifulSoup
from psycopg2 import OperationalError
from config.db_config import create_connection
from config.paths_config import urls_to_pricing_module
from config.time_config import time_format

log_directory = urls_to_pricing_module
os.makedirs(log_directory, exist_ok=True)
date = datetime.date.today()
log_file_path = os.path.join(log_directory, f'urls_to_pricing_module_{date}.log')

logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    filemode='w',
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt=time_format
)


def get_pricing_urls(connection):
    query = "SELECT url from site_tree"
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        logging.info(f"Data Extracted from site_tree table. QUERY: {query} ")
        result = cursor.fetchall()
    except OperationalError as e:
        logging.error(f"Error has happend: {e}")
    logging.info(f"Starting processing data")

    for row in result:
        url = row[0]
        response = requests.get(url)
        soup = BeautifulSoup(response.text, "html.parser")
        if soup.find("div", class_="nums"):
            number_of_pages = int(soup.find("div", class_="nums").find_all("a", class_="dark_link")[-1].text)
            process_multiple_pages_data(url, number_of_pages, connection)
        else:
            process_single_page_data(url, connection)


def process_multiple_pages_data(url, number_of_pages, connection):
    base_category_url = url
    page_param = f'?PAGEN_1='
    response = requests.get(base_category_url)
    soup = BeautifulSoup(response.text, "html.parser")
    category_name = soup.find("div", class_='topic__heading').find("h1").get_text()
    urls_to_crawl = [f'{url}{page_param}{gen_url}' for gen_url in range(2, number_of_pages + 1)]
    urls_to_crawl.insert(0, url)
    pricing_urls = []
    for url in urls_to_crawl:
        response = requests.get(url)
        soup = BeautifulSoup(response.text, "html.parser")
        products_links = soup.find_all("div", class_='inner_wrap TYPE_1')
        for link in products_links:
            product_link = 'https://bellaktshop.by' + link.find('a').get('href')
            pricing_urls.append(product_link)
            logging.info(f'Product link: {product_link} - added| BASEURL {base_category_url}')

    if pricing_urls:
        date_of_insertion = datetime.date.today()
        try:
            with connection.cursor() as cur:
                for url in pricing_urls:
                    try:
                        # Проверка существующих записей
                        cur.execute("SELECT COUNT(*) FROM urls_to_crawling WHERE pricing_url = %s AND category_name = "
                                    "%s AND date::date = %s",
                                    (url, category_name, date_of_insertion))
                        exists = cur.fetchone()[0] > 0

                        if exists:
                            # Обновление существующей записи
                            cur.execute(
                                "UPDATE urls_to_crawling SET pricing_url = %s, date = %s, category_name = %s "
                                "WHERE pricing_url = %s AND date::date = %s AND category_name = %s",
                                (url, date_of_insertion, category_name, url, date_of_insertion, category_name)
                            )
                            logging.info(f"Updated data for URL: {url}")
                        else:
                            # Вставка новой записи
                            cur.execute(
                                "INSERT INTO urls_to_crawling (pricing_url, date, category_name) VALUES (%s, %s, %s)",
                                (url, date_of_insertion, category_name)
                            )
                            logging.info(f"Inserted data for URL: {url}")
                    except Exception as e:
                        logging.error(f"The error occurred in block with: {e}")
                        connection.rollback()
            connection.commit()  # Выполняем коммит после всех вставок
            logging.info("Data insertion completed.")
        except Exception as e:
            logging.error(f"The error occurred in block try: {e}")
            connection.rollback()


def process_single_page_data(url, connection):
    base_category_url = url
    response = requests.get(base_category_url)
    soup = BeautifulSoup(response.text, "html.parser")
    products_links = soup.find_all("div", class_='inner_wrap TYPE_1')
    pricing_urls = []
    for link in products_links:
        product_link = 'https://bellaktshop.by' + link.find('a').get('href')
        pricing_urls.append(product_link)
        logging.info(f'Product link: {product_link} - added| BASEURL {base_category_url}')

    if pricing_urls:
        date_of_insertion = datetime.date.today()
        with connection.cursor() as cur:
            for url in pricing_urls:
                pricing_url = url
                try:
                    cur.execute(
                            "SELECT COUNT(*) FROM urls_to_crawling WHERE pricing_url = %s AND date::date = %s",
                            (url, date_of_insertion)
                    )
                    exists = cur.fetchone()[0] > 0
                    if exists:
                        # Обновление существующей записи
                        cur.execute(
                            "UPDATE urls_to_crawling SET date = %s WHERE pricing_url = %s AND date::date = %s",
                            (date_of_insertion, url, date_of_insertion))
                        logging.info(f"UPDATED data for URL: {url}")
                    else:
                        # Вставка новой записи
                        cur.execute(
                            "INSERT INTO urls_to_crawling (pricing_url, date) VALUES (%s, %s)",
                            (pricing_url, date_of_insertion)
                        )
                    logging.info(f"Inserted data for category_name {url}")
                except Exception as e:
                    logging.error(f"The error occurred in block with in process_single_page_data: {e}")
                    connection.rollback()
        connection.commit()  # Выполняем коммит после успешной вставки
    else:
        logging.info(f"{pricing_urls}, EMPTY, {url}.")

    # logging.info("Data insertion completed in process_single_page_data.")


# Создание соединения и вызов функции
connect = create_connection()
get_pricing_urls(connect)
