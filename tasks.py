import logging
from config.celery_conf import app
from customers.bellakt.ranking.cr_tree_bellaktshop_by import GetCRTree
from customers.bellakt.ranking.site_set_bellaktshop_by import SiteSet
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

@app.task(bind=True, max_retries=3)
def get_site_set_task(self,url):
    try:
        site_site = SiteSet(url)
        site_site.get_categories()
        # logging.info(f"Categories saved to file: {file_path}")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        self.retry(exc=e, countdown=60)


app.conf.update(
    beat_schedule={
        'get-cr-tree-categories-task': {
            'task': 'tasks.get_cr_tree_categories_task',
            'schedule': crontab(hour='22', minute='45'),  # Выполнять в 21:30
            'args': [main_url],  # Передача аргументов в задачу
        },
        'get-site-set-task': {
            'task': 'tasks.get_site_set_task',
            'schedule': crontab(hour='22', minute='50'),  # Выполнять в 21:30
            'args': [main_url],  # Передача аргументов в задачу
        }
    }
)