import logging
import time
import requests
from config.celery_conf import app
from config.db_config import create_connection
from customers.bellakt.ranking.site_set_bellaktshop_by import clear_table, get_categories
from customers.bellakt.ranking.cr_tree_bellaktshop_by import get_cr_tree_categories
from celery.schedules import crontab


main_url = "https://bellaktshop.by/catalog"


@app.task
def run_tasks():
    # Создание нового соединения с базой данных внутри задачи
    connect = create_connection()

    try:
        get_cr_tree_categories(main_url)
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 503:
            logging.error("Сервер недоступен, повторная попытка через 10 секунд.")
            time.sleep(10)  # Ждем 10 секунд перед повторной попыткой
            run_tasks.apply_async()  # Повторяем задачу


app.conf.update(
    CELERY_TIMEZONE='UTC',
    CELERYBEAT_SCHEDULE={
        'my-task-every-day': {
            'task': 'tasks.run_tasks',
            'schedule': crontab(minute='*/1  * * * *'),  # Выполнять каждый день в 8 утра (minute=0, hour=8)
        },
    }
)

# try:
#     clear_table(connect)
#     if get_categories(main_url, connect):
#         logging.info("get_categories выполнена успешно.")
#         # Список функций для вызова
#         functions_to_call = [
#             function_1,
#             function_2,
#             function_3,
#
#         ]
#
#         # Вызов каждой функции из списка последовательно
#         for func in functions_to_call:
#             try:
#                 func(connect)  # Передаем соединение как аргумент
#                 logging.info(f"{func.__name__} выполнена успешно.")
#             except Exception as e:
#                 logging.error(f"Ошибка при выполнении {func.__name__}: {e}")
#                 break  # Прерываем выполнение, если произошла ошибка
#     else:
#         logging.error("get_categories не выполнена успешно.")
# except requests.exceptions.HTTPError as e:
#     if e.response.status_code == 503:
#         logging.error("Сервер недоступен, повторная попытка через 10 секунд.")
#         time.sleep(10)  # Ждем 10 секунд перед повторной попыткой
#         run_tasks.apply_async()  # Повторяем задачу
# except Exception as e:
#     logging.error(f"Ошибка: {e}")
