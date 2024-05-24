import psycopg2
from psycopg2 import OperationalError
import os
import logging
from dotenv import load_dotenv

load_dotenv()


class Database:
    def __init__(self):
        self.conn = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(
                dbname=os.getenv("POSTGRES_DB"),
                user=os.getenv("POSTGRES_USER"),
                password=os.getenv("POSTGRES_PASSWORD"),
                host=os.getenv("POSTGRES_HOST"),
                port="5432"
            )
            logging.info("Database connected successfully.")
        except OperationalError as e:
            logging.error(f"Error connecting to database: {e}")
            self.conn = None
        return self.conn

    def execute_query(self, query, params=None, fetch=False):
        if self.conn is None:
            logging.error("No database connection.")
            return None
        try:
            cur = self.conn.cursor()
            cur.execute(query, params)
            if fetch:
                result = cur.fetchall()
                cur.close()
                return result
            else:
                self.conn.commit()
                cur.close()
                return None
        except psycopg2.Error as e:
            logging.error(f"Database query error: {e}")
            return None

    def commit(self):
        if self.conn:
            try:
                self.conn.commit()
            except psycopg2.Error as e:
                logging.error(f"Error committing to database: {e}")

    def close(self):
        if self.conn:
            self.conn.close()
            logging.info("Database connection closed.")
