import datetime
import requests
from bs4 import BeautifulSoup
import logging
import os
from config.db_config import create_connection
from config.paths_config import site_set


connect = create_connection()
date = datetime.date.today()

log_directory = site_set
os.makedirs(log_directory, exist_ok=True)
log_file_path = os.path.join(log_directory, f'site_set_{date}.log')

logging.basicConfig(filename=log_file_path, level=logging.INFO, filemode='w', format='- %(levelname)s - %(message)s')


def clear_table(connection):
    try:
        with connection.cursor() as cur:
            cur.execute("TRUNCATE TABLE site_tree RESTART IDENTITY CASCADE;")
            connection.commit()
            logging.info('Таблица site_tree очищена.')
    except Exception as e:
        logging.error(f"Ошибка при очистке таблицы: {str(e)}")


def get_categories(url, connection):
    clear_table(connect)

    logging.info('Начало обработки...')

    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        categories_tree = []

        links = soup.find_all("div", class_='item_block lg col-lg-20 col-md-4 col-xs-6')
        for link in links:
            category_name = link.find('span').get_text().strip()
            category_link = link.find('a').get('href')
            category_url = 'https://bellaktshop.by' + category_link

            # Добавляем основную категорию
            main_category = f"Каталог *** {category_name}"
            categories_tree.append(main_category)
            current_time = datetime.datetime.now(datetime.timezone.utc)

            cur = connection.cursor()
            cur.execute(
                "INSERT INTO site_tree (shop_id, name, url, site_set, created_at) VALUES (%s, %s, %s, %s, %s)",
                ("101", main_category, category_url, 'bellakt', current_time))
            connection.commit()
            cur.close()

            # Обрабатываем все уровни вложенности
            process_category(category_url, categories_tree, main_category, connection)
    except requests.exceptions.RequestException as e:
        logging.error(f"Ошибка при запросе: {str(e)}")
    except Exception as e:
        logging.error(f"Произошла ошибка: {str(e)}")
    finally:
        connection.close()
        logging.info('Завершение обработки.')


def process_category(category_url, categories_tree, parent_category, connection):
    logging.info(f"Обработка категории: {category_url}")

    try:
        response = requests.get(category_url)
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

            cur = connection.cursor()
            cur.execute(
                "INSERT INTO site_tree (shop_id, name, url, site_set, created_at) VALUES (%s, %s, %s, %s, "
                "%s)", ("101", category_name, sub_category_url, 'bellakt', current_time))

            connection.commit()
            cur.close()

            # Рекурсивно обрабатываем подкатегории
            subcategory_link = sub_category.find('a').get('href')
            subcategory_url = 'https://bellaktshop.by' + subcategory_link
            process_category(subcategory_url, categories_tree, category_name, connection)

    except requests.exceptions.RequestException as e:
        logging.error(f"Ошибка при запросе подкатегорий: {str(e)}")
    except Exception as e:
        logging.error(f"Произошла ошибка при обработке подкатегорий: {str(e)}")


main_url = "https://bellaktshop.by/catalog"

clear_table(connect)
get_categories(main_url, connect)
