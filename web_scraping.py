import time
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import logging

class WebScraper:
    def __init__(self, base_url):
        self.base_url = base_url

    def scrape_page(self, url):
        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page()
            page.set_default_navigation_timeout(180000)

            try:
                page.goto(url)
                page_content = page.content()
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
            finally:
                browser.close()

    def extract_job_details(self, soup):
        job_details = []
        table = soup.find('table', id='temp_table')
        for table_row in table:
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
