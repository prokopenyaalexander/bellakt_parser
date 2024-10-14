import logging
import time
import requests
from config.celery_conf import app
from config.db_config import create_connection
from customers.bellakt.pc.pc_bellakt import PC
from customers.bellakt.pricing.add_urls_to_pricing_module import UrlsToCrawl
from customers.bellakt.pricing.pricing_bellakt import Pricing
from customers.bellakt.ranking.get_count_ranking_products import RankProds
from customers.bellakt.ranking.site_set_bellaktshop_by import clear_table, SiteSet
from customers.bellakt.ranking.cr_tree_bellaktshop_by import GetCRTree
from celery.schedules import crontab


main_url = "https://bellaktshop.by/catalog"

# Запускать команду в 15:00 каждый день с понедельника по пятницу:
# 0 15 * * 1-5 command

@app.task(bind=True, default_retry_delay=10, max_retries=5)
def get_cr_tree_categories_task(self, url):
    try:
        cr_tree = GetCRTree(url)
        file_path = cr_tree.get_cr_tree_categories()
        logging.info(f"Categories saved to file: {file_path}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching URL: {e}")
        self.retry(exc=e)

@app.task
def get_site_set_categories_task(url):
    try:
        site_set = SiteSet(url)
        site_set.get_categories()
        logging.info(f"Categories processing completed for URL: {url}")
    except Exception as e:
        logging.error(f"Error processing categories: {str(e)}")

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
        logging.info(f"urls are wrote to DB")
    except Exception as e:
        logging.error(f"Error processing urls: {str(e)}")

@app.task
def pc_bellakt():
    try:
        details = PC()
        details.get_pc_details()
        details.find_duplicates()
        details.remove_duplicates()
        logging.info(f"details are wrote to DB")
    except Exception as e:
        logging.error(f"Error processing details: {str(e)}")

app.conf.update(
    CELERY_TIMEZONE='UTC',
    CELERYBEAT_SCHEDULE={
        'my-task-every-day': {
            'task': 'tasks.get_cr_tree_categories_task',
            'schedule': crontab(minute='0  12 * * *'),  # Выполнять в 12
            'args': (main_url,),  # Передача аргументов в задачу
        },
        'site-set-categories-task': {
            'task': 'tasks.get_site_set_categories_task',
            'schedule': crontab(minute='15  12 * * *'),  # Выполнять в 12:15
            'args': ('https://bellaktshop.by/catalog',),  # Передача аргументов в задачу
        },
        'get-count-ranking-products-task': {
            'task': 'tasks.get_count_ranking_products_task',
            'schedule': crontab(minute='30  12 * * *'),  # Выполнять в 12:30
        },
        'add-urls-to-pricing-module-task': {
            'task': 'tasks.add_urls_to_pricing_module_task',
            'schedule': crontab(minute='45  12 * * *'),  # Выполнять в 12:45
        },
        'pricing-bellakt-task': {
            'task': 'tasks.pricing_bellakt_task',
            'schedule': crontab(minute='00  13 * * *'),  # Выполнять в 13:00
        },
        'pc-bellakt': {
            'task': 'tasks.pc_bellakt',
            'schedule': crontab(minute='15  13 * * *'),  # Выполнять в 13:00
        },
    }
)

