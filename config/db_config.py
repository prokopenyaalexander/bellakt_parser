import os
import psycopg2
from dotenv import load_dotenv


success = load_dotenv("/home/alex/Documents/projects/profidata/.env")


def create_connection():
    try:
        connection = psycopg2.connect(
            dbname=os.environ.get('DB_NAME'),
            user=os.environ.get('DB_USER'),
            password=os.environ.get('DB_PASS'),
            host=os.environ.get('DB_HOST'),
            port=os.environ.get('DB_PORT')
        )
        print("Connection to PostgreSQL DB successful")
        return connection
    except psycopg2.OperationalError as e:
        print(f" '{e}' ")
