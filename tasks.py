import logging
import requests
from config.celery_conf import app
from customers.bellakt.pc.pc_bellakt import PC
from customers.bellakt.pricing.add_urls_to_pricing_module import UrlsToCrawl
from customers.bellakt.pricing.pricing_bellakt import Pricing
from customers.bellakt.ranking.get_count_ranking_products import RankProds
from customers.bellakt.ranking.site_set_bellaktshop_by import clear_table, SiteSet
from customers.bellakt.ranking.cr_tree_bellaktshop_by import GetCRTree
from celery.schedules import crontab

main_url = "https://bellaktshop.by/catalog"

@app.task(bind=True, default_retry_delay=10, max_retries=5)
def get_cr_tree_categories_task(self, url):
    try:
        cr_tree = GetCRTree(url)
        file_path = cr_tree.get_cr_tree_categories()
        logging.info(f"Categories saved to file: {file_path}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching URL: {e}")
        self.retry(exc=e)
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")

@app.task
def get_site_set_categories_task(url):
    try:
        site_set = SiteSet(url)
        site_set.get_categories()
        logging.info(f"Categories processing completed for URL: {url}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching URL: {e}")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")

@app.task
def get_count_ranking_products_task():
    try:
        get_count = RankProds()
        get_count.get_products_count()
        get_count.find_duplicates()
        get_count.remove_duplicates()
        logging.info(f"Count of products are wrote to DB")
    except Exception as e:
        logging.error(f"Error processing Count of products: {str(e)}")

@app.task
def add_urls_to_pricing_module_task():
    try:
        urls = UrlsToCrawl()
        urls.get_pricing_urls()
        urls.find_duplicates()
        urls.remove_duplicates()
        logging.info(f"urls are wrote to DB")
    except Exception as e:
        logging.error(f"Error processing urls: {str(e)}")

@app.task
def pricing_bellakt_task():
    try:
        details = Pricing()
        details.get_pricing_details()
        details.find_duplicates()
        details.remove_duplicates()
        logging.info(f"Pricing details are wrote to DB")
    except Exception as e:
        logging.error(f"Error processing pricing details: {str(e)}")

@app.task
def pc_bellakt_task(): # Добавлено '_task' в название функции
    try:
        details = PC()
        details.get_pc_details()
        details.find_duplicates()
        details.remove_duplicates()
        logging.info(f"PC details are wrote to DB")
    except Exception as e:
        logging.error(f"Error processing PC details: {str(e)}")

app.conf.update(
    CELERY_TIMEZONE='UTC',
    CELERYBEAT_SCHEDULE={
        'my-task-every-day': {
            'task': 'tasks.get_cr_tree_categories_task',
            'schedule': crontab(hour='21', minute='30'),  # Выполнять в 21:30
            'args': (main_url,),  # Передача аргументов в задачу
        },
        'site-set-categories-task': {
            'task': 'tasks.get_site_set_categories_task',
            'schedule': crontab(hour='21', minute='40'),  # Выполнять в 21:35
            'args': ('https://bellaktshop.by/catalog',),  # Передача аргументов в задачу
        },
        'get-count-ranking-products-task': {
            'task': 'tasks.get_count_ranking_products_task',
            'schedule': crontab(hour='21', minute='45'),  # Выполнять в 21:40
        },
        'add-urls-to-pricing-module-task': {
            'task': 'tasks.add_urls_to_pricing_module_task',
            'schedule': crontab(hour='21', minute='50'),  # Выполнять в 21:45
        },
        'pricing-bellakt-task': {
            'task': 'tasks.pricing_bellakt_task',
            'schedule': crontab(hour='21', minute='55'),  # Выполнять в 21:50
        },
        'pc-bellakt-task': { # Добавлено '_task' в название ключа
            'task': 'tasks.pc_bellakt_task',
            'schedule': crontab(hour='21', minute='55'),  # Выполнять в 21:55
        },
    }
)