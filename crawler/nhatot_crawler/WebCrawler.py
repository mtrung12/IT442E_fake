from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
import time
from logger import log

class WebCrawler:
    def __init__(self, base_url, user_agent):
        self.base_url = base_url
        self.user_agent = user_agent

    def fetch_page(self, url):
        try:
            options = Options()
            options.add_argument('--headless=new')  
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-gpu')
            options.add_argument('--window-size=1920,1080')
            options.add_argument(f'--user-agent={self.user_agent}')
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option('useAutomationExtension', False)

            driver = webdriver.Chrome(options=options)
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => false});")

            driver.get(url)
            time.sleep(3)  
            html = driver.page_source
            driver.quit()
            return html
        except Exception as e:
            log.error(f"Error fetching {url} with Selenium: {e}")
            return None

    def parse_listing_page(self, html):
        soup = BeautifulSoup(html, 'html.parser')
        ad_links = soup.find_all('a', href=True)
        ids = []
        for link in ad_links:
            href = link['href']
            if href.startswith('/mua-ban-') and href.endswith('.htm'):
                parts = href.split('/')[-1].split('.htm')[0].split('-')[-1]
                if parts.isdigit():
                    ids.append(int(parts))
        return ids

    def crawl_page(self, page): 
        url = f"{self.base_url}?page={page}"
        html = self.fetch_page(url)
        if not html:
            return []
        return self.parse_listing_page(html)