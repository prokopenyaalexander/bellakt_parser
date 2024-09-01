import logging
import time
from config.headers import header
import requests
from config.celery_conf import app
from config.db_config import create_connection
from customers.bellakt.ranking.site_set_bellaktshop_by import clear_table, get_categories
from celery.schedules import crontab


main_url = "https://bellaktshop.by/catalog"


@app.task
def run_tasks():
    # Создание нового соединения с базой данных внутри задачи
    connect = create_connection()

    try:
        clear_table(connect)
        get_categories(main_url, connect)
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 503:
            logging.error("Сервер недоступен, повторная попытка через 10 секунд.")
            time.sleep(10)  # Ждем 10 секунд перед повторной попыткой
            run_tasks.apply_async()  # Повторяем задачу


app.conf.update(
    CELERY_TIMEZONE='UTC',
    CELERYBEAT_SCHEDULE={
        'my-task-every-minute': {
            'task': 'tasks.run_tasks',
            'schedule': crontab(minute='*/5  * * * *'),  # Выполнять каждые 5 минут
        },
    }
)
