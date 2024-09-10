import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
import logging
import datetime
from config.time_config import time_format
from config.paths_config import db_connection_log_directory

date = datetime.date.today()
log_directory = db_connection_log_directory
os.makedirs(log_directory, exist_ok=True)
log_file_path = os.path.join(log_directory, f'db_connection-{date}.log')

logger = logging.getLogger('dbConnectionLogger')
logger.setLevel(logging.INFO)
handler = logging.FileHandler(log_file_path, mode='w')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt=time_format)
handler.setFormatter(formatter)
logger.addHandler(handler)

try:
    success_load_env = load_dotenv("/home/alex/Documents/projects/profidata/.env")
    logger.info('load_dotenv variables loaded')
except Exception as e:
    logger.error(f"Something went wrong. load_dotenv variables not loaded {str(e)}")

database = os.environ.get('DB_NAME')
username = os.environ.get('DB_USER')
password = os.environ.get('DB_PASS')
host = os.environ.get('DB_HOST')
port = os.environ.get('DB_PORT')

# Creation of connection_string
connection_string = f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}'

# create_engine
engine = create_engine(connection_string)


# DB Connection
def create_connection():
    logging.info('Start connecting')
    try:
        with engine.connect() as connection:
            logger.info('Connection to PostgreSQL DB successful')
        return connection
    except Exception as error_in_connection:
        logger.error(f'Something went wrong in connection - {error_in_connection}')
