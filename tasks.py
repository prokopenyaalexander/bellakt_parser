import logging
from config.celery_conf import app
from customers.bellakt.ranking.cr_tree_bellaktshop_by import GetCRTree
from celery.schedules import crontab

main_url = "https://bellaktshop.by/catalog"


@app.task(bind=True, max_retries=3)
def get_cr_tree_categories_task(self, url):
    try:
        cr_tree = GetCRTree(url)
        file_path = cr_tree.get_cr_tree_categories()
        logging.info(f"Categories saved to file: {file_path}")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        self.retry(exc=e)

app.conf.update(
    beat_schedule={
        'my-task-every-day': {
            'task': 'tasks.get_cr_tree_categories_task',
            'schedule': crontab(hour='12', minute='37'),  # Выполнять в 21:30
            'args': [main_url],  # Передача аргументов в задачу
        }
    }
)