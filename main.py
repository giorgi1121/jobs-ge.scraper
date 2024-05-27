import asyncio
import time
import logging
from database import Database
from web_scraping import WebScraper

BASE_URL = 'https://jobs.ge'

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class JobScraper:
    def __init__(self, base_url):
        self.database = Database()
        self.scraper = WebScraper(base_url)
        self.unique_job_urls = set()
        self.jobs = []

    def load_existing_job_urls(self):
        url_query = "SELECT job_url FROM jobs"
        existing_job_urls = self.database.execute_query(url_query, fetch=True)
        if existing_job_urls and len(existing_job_urls) > 0:
            self.unique_job_urls = set(url[0] for url in existing_job_urls)
        logging.info(f"Loaded {len(self.unique_job_urls)} unique job URLs from the database.")

    def insert_job(self, job):
        try:
            self.database.execute_query(
                "INSERT INTO jobs (vacancy_name, company_name, published, deadline, job_url) VALUES (%s, %s, %s, %s, %s)",
                (job['vacancy_name'], job['company_name'], job['published'], job['deadline'], job['job_url'])
            )
        except Exception as e:
            logging.error(f"Error inserting job into database: {e}")

    async def async_insert_job(self, job):
        return await asyncio.to_thread(self.insert_job, job)

    async def scrape_jobs(self, page_number):
        url = f"{BASE_URL}/en/?page={page_number}&q=&cid=0&lid=0&jid=0&in_title=0&has_salary=0&is_ge=0&for_scroll=yes"
        logging.info(f"Scraping page {page_number}: {url}")
        soup = await self.scraper.scrape_page(url)

        if soup is None:
            logging.warning("Failed to retrieve the page or no content found. Stopping scraping.")
            return

        job_details = self.scraper.extract_job_details(soup)
        if not job_details:
            logging.info("No job details found on the current page. Stopping scraping.")
            return

        for job in job_details:
            if job['job_url'] in self.unique_job_urls:
                logging.info(f"Duplicate job URL found: {job['job_url']}. Skipping insertion.")
                continue
            self.unique_job_urls.add(job['job_url'])
            self.jobs.append(job)

        self.database.commit()
        return self.jobs

    async def run(self):
        start_time = time.time()
        self.database.connect()
        self.load_existing_job_urls()
        tasks = [self.scrape_jobs(i) for i in range(1, 2)]
        all_jobs = await asyncio.gather(*tasks)
        task_to_list = []
        for each_page_job in all_jobs:
            for job in each_page_job:
                task_to_list.append(self.insert_job(job))
        await asyncio.gather(*task_to_list)
        self.database.close()
        self.scraper.close()
        end_time = time.time()
        execution_time = end_time - start_time
        logging.info(f"Total execution time: {execution_time:.2f} seconds")


async def run_scraper():
    job_scraper_instance = JobScraper(BASE_URL)
    await job_scraper_instance.run()


if __name__ == "__main__":
    asyncio.run(run_scraper())
