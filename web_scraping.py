import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait  # Import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import logging
import requests
import asyncio
import concurrent.futures
from functools import partial


class WebScraper:
    def __init__(self, base_url):
        self.base_url = base_url
        chrome_options = Options()
        chrome_options.add_experimental_option('detach', True)
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

    def scrape_page(self, url):
        try:
            response = requests.get(url)
            if response.status_code != 200:
                logging.error(f"Error: Unable to retrieve page content, status code {response.status_code}.")
                return None
            page_content = response.text
            if not page_content:
                logging.error("Error: Page content not retrieved.")
                return None
            soup = BeautifulSoup(page_content, 'html.parser')
            if not soup:
                logging.error("Error: Unable to parse page content.")
                return None
            return soup
        except Exception as e:
            logging.error(f"Error: {e}")
            return None

    def extract_job_details(self, soup):
        job_details = []
        table = soup.find('table', id='temp_table')
        if not table:
            logging.error("Error: No table found with id 'temp_table'.")
            return job_details

        tr_tags = table.find_all("tr")
        for tr_tag in tr_tags:
            td_tags = tr_tag.find_all('td')
            if len(td_tags) >= 2:
                vacancy_name = td_tags[1].get_text(strip=True)
                company_name = td_tags[3].get_text(strip=True)
                published = td_tags[4].get_text(strip=True)
                deadline = td_tags[5].get_text(strip=True)
                a_tag = td_tags[1].find('a')
                if a_tag and 'href' in a_tag.attrs:
                    href_value = a_tag['href']
                    job_url = urljoin(self.base_url, href_value)
                    job_details.append({
                        'vacancy_name': vacancy_name,
                        'company_name': company_name,
                        'published': published,
                        'deadline': deadline,
                        'job_url': job_url
                    })
        return job_details

    def close(self):
        self.driver.quit()
