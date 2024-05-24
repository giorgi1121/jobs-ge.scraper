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
        self.scraped_pages = set()
        self.previous_job_details = []

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

    def scrape_jobs(self):
        page_number = 1
        while True:
            if page_number in self.scraped_pages:
                logging.info(f"Page {page_number} has already been scraped. Stopping to avoid looping.")
                break

            url = f"{BASE_URL}/en/?page={page_number}&q=&cid=0&lid=0&jid=0&in_title=0&has_salary=0&is_ge=0&for_scroll=yes"
            logging.info(f"Scraping page {page_number}: {url}")
            soup = self.scraper.scrape_page(url)
            if soup is None:
                logging.warning("Failed to retrieve the page or no content found. Stopping scraping.")
                break

            job_details = self.scraper.extract_job_details(soup)
            if not job_details:
                logging.info("No job details found on the current page. Stopping scraping.")
                break

            if job_details == self.previous_job_details:
                logging.info("Detected looping of job listings. Stopping scraping.")
                break

            self.previous_job_details = job_details
            self.scraped_pages.add(page_number)

            for job in job_details:
                if job['job_url'] in self.unique_job_urls:
                    logging.info(f"Duplicate job URL found: {job['job_url']}. Skipping insertion.")
                    continue
                self.unique_job_urls.add(job['job_url'])
                self.insert_job(job)

            self.database.commit()
            page_number += 1
            time.sleep(2)  # Throttle requests to avoid overloading the server

    def run(self):
        start_time = time.time()
        self.database.connect()
        self.load_existing_job_urls()
        self.scrape_jobs()
        self.database.close()
        self.scraper.close()
        end_time = time.time()
        execution_time = end_time - start_time
        logging.info(f"Total execution time: {execution_time:.2f} seconds")


def run_scraper():
    job_scraper_instance = JobScraper(BASE_URL)
    job_scraper_instance.run()

if __name__ == "__main__":
    job_scraper = JobScraper(BASE_URL)
    job_scraper.run()
