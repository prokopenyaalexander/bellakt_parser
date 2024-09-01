from config.celery_conf import app
from config.db_config import create_connection
from customers.bellakt.ranking.site_set_bellaktshop_by import clear_table, get_categories
from celery.schedules import crontab


main_url = "https://bellaktshop.by/catalog"


@app.task
def run_tasks():
    # Создание нового соединения с базой данных внутри задачи
    connect = create_connection()

    # Выполнение очистки таблицы
    clear_table(connect)

    # Выполнение сбора категорий
    get_categories(main_url, connect)


app.conf.update(
    CELERY_TIMEZONE='UTC',
    CELERYBEAT_SCHEDULE={
        'my-task-every-minute': {
            'task': 'tasks.run_tasks',
            'schedule': crontab(minute='*/5  * * * *'),  # Выполнять каждые 5 минут
        },
    }
)
