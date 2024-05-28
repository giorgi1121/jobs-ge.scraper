import os
import itertools
import asyncio
import logging
import time
from database import AsyncDatabase
from web_scraping import WebScraper

BASE_URL = 'https://jobs.ge'

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class JobScraper:
    def __init__(self, base_url):
        self.database = AsyncDatabase(
            f'postgresql://{os.getenv("POSTGRES_USER")}:{os.getenv("POSTGRES_PASSWORD")}@{os.getenv("POSTGRES_HOST")}:{os.getenv("POSTGRES_PORT")}/{os.getenv("POSTGRES_DB")}'
        )
        self.scraper = WebScraper(base_url)
        self.unique_job_urls = set()

    async def load_existing_job_urls(self):
        url_query = "SELECT job_url FROM jobs"
        existing_job_urls = await self.database.execute_query(url_query, fetch=True)
        if existing_job_urls and len(existing_job_urls) > 0:
            self.unique_job_urls = set(url[0] for url in existing_job_urls)
        logging.info(f"Loaded {len(self.unique_job_urls)} unique job URLs from the database.")

    async def insert_job(self, job):
        try:
            await self.database.execute_query(
                "INSERT INTO jobs (vacancy_name, company_name, published, deadline, job_url) VALUES (:vacancy_name, :company_name, :published, :deadline, :job_url)",
                params=job
            )
        except Exception as e:
            logging.error(f"Error inserting job into database: {e}")

    async def scrape_jobs(self, page_number):
        url = f"{BASE_URL}/en/?page={page_number}&q=&cid=0&lid=0&jid=0&in_title=0&has_salary=0&is_ge=0&for_scroll=yes"
        logging.info(f"Scraping page {page_number}: {url}")
        soup = await self.scraper.scrape_page(url)

        if soup is None:
            logging.warning("Failed to retrieve the page or no content found. Stopping scraping.")
            return []

        job_details = self.scraper.extract_job_details(soup)
        if not job_details:
            logging.info("No job details found on the current page. Stopping scraping.")
            return []

        jobs = []
        for job in job_details:
            if job['job_url'] in self.unique_job_urls:
                logging.info(f"Duplicate job URL found: {job['job_url']}. Skipping insertion.")
                continue
            self.unique_job_urls.add(job['job_url'])
            jobs.append(job)

        logging.info(f"Number of jobs scraped on page {page_number}: {len(jobs)}")
        return jobs

    async def run(self):
        start_time = time.time()
        await self.database.connect()
        await self.load_existing_job_urls()
        tasks = [self.scrape_jobs(i) for i in range(1, 21)]  # Scraping pages from 1 to 2 (inclusive)
        all_jobs = await asyncio.gather(*tasks)
        all_jobs_flat = list(itertools.chain.from_iterable(all_jobs))
        logging.info(f"Number of jobs: {len(all_jobs_flat)}")

        tasks_to_insert = []
        for job in all_jobs_flat:
            tasks_to_insert.append(self.insert_job(job))
            logging.info(f"Added job to tasks_to_insert: {job['job_url']}")
        await asyncio.gather(*tasks_to_insert)  # Insert all jobs asynchronously
        await self.database.close()
        self.scraper.close()
        end_time = time.time()
        execution_time = end_time - start_time
        logging.info(f"Total execution time: {execution_time:.2f} seconds")


async def run_scraper():
    job_scraper_instance = JobScraper(BASE_URL)
    await job_scraper_instance.run()

if __name__ == "__main__":
    asyncio.run(run_scraper())
