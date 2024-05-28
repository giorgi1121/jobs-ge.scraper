import os
import logging
from databases import Database


class AsyncDatabase:
    def __init__(self, db_url):
        self.db = Database(db_url)

    async def connect(self):
        try:
            await self.db.connect()
            logging.info("Database connected successfully.")
        except Exception as e:
            logging.error(f"Error connecting to database: {e}")

    async def execute_query(self, query, params=None, fetch=False):
        try:
            async with self.db.transaction():
                if fetch:
                    result = await self.db.fetch_all(query, values=params)
                    return result
                else:
                    await self.db.execute(query, values=params)
        except Exception as e:
            logging.error(f"Database query error: {e}")

    async def commit(self):
        pass  # No need for explicit commit with the databases library

    async def close(self):
        try:
            await self.db.disconnect()
            logging.info("Database connection closed.")
        except Exception as e:
            logging.error(f"Error closing database connection: {e}")
