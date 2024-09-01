import datetime
import requests
from bs4 import BeautifulSoup
import logging
import os
from config.db_config import create_connection
from config.paths_config import site_set
from config.headers import header
from config.time_config import time_format


date = datetime.date.today()
log_directory = site_set
os.makedirs(log_directory, exist_ok=True)
log_file_path = os.path.join(log_directory, f'site_set_{date}.log')

logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    filemode='w',
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt=time_format
)


def get_categories(url, connection):
    clear_table(connect)
    logging.info('Starting work')
    try:
        response = requests.get(url, headers=header)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        categories_tree = []
        links = soup.find_all("div", class_='item_block lg col-lg-20 col-md-4 col-xs-6')
        for link in links:
            category_name = link.find('span').get_text().strip()
            category_link = link.find('a').get('href')
            category_url = 'https://bellaktshop.by' + category_link

            main_category = f"Каталог *** {category_name}"  # Добавляем основную категорию
            categories_tree.append(main_category)
            logging.info(f'MAIN CATEGORY ADDED {main_category} URL - {category_url}')
            current_time = datetime.datetime.now(datetime.timezone.utc)

            write_to_db(connection, main_category, category_url, current_time)

            logging.info(f'Added record {main_category}, {category_url}')

            # Обрабатываем все уровни вложенности
            process_category(category_url, categories_tree, main_category, connection)
    except requests.exceptions.RequestException as e:
        logging.error(f"Ошибка при запросе: {str(e)}")
    except Exception as e:
        logging.error(f"Произошла ошибка: {str(e)}")
    logging.info('Завершение обработки.')


def process_category(category_url, categories_tree, parent_category, connection):
    logging.info(f"Обработка категории: {category_url}")
    try:
        response = requests.get(category_url, headers=header)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        sub_categories = soup.find_all('div', class_='col-lg-3 col-md-4 col-xs-6 col-xxs-12')

        for sub_category in sub_categories:
            sub_category_name = sub_category.find('span').get_text().strip()
            sub_category_link = sub_category.find('a').get('href')
            sub_category_url = 'https://bellaktshop.by' + sub_category_link
            category_name = f"{parent_category} *** {sub_category_name}"
            categories_tree.append(category_name)
            current_time = datetime.datetime.now(datetime.timezone.utc)

            write_to_db(connection, category_name, sub_category_url, current_time)
            logging.info(f'Added record {category_name}, {category_url}')

            # Рекурсивно обрабатываем подкатегории
            subcategory_link = sub_category.find('a').get('href')
            subcategory_url = 'https://bellaktshop.by' + subcategory_link
            process_category(subcategory_url, categories_tree, category_name, connection)

    except requests.exceptions.RequestException as e:
        logging.error(f"Ошибка при запросе подкатегорий: {str(e)}")
    except Exception as e:
        logging.error(f"Произошла ошибка при обработке подкатегорий: {str(e)}")


def clear_table(connection):
    try:
        with connection.cursor() as cur:
            cur.execute("TRUNCATE TABLE site_tree RESTART IDENTITY CASCADE;")
            connection.commit()
            logging.info('Таблица site_tree очищена.')
    except Exception as e:
        logging.error(f"Ошибка при очистке таблицы: {str(e)}")


def write_to_db(connection, name, url, date_time):
    with connection.cursor() as cur:
        cur.execute(
            "INSERT INTO site_tree (name, url, created_at) VALUES (%s, %s, %s)",
            (name, url, date_time))
    connection.commit()


connect = create_connection()
# main_url = "https://bellaktshop.by/catalog"
# get_categories(main_url, connect)
