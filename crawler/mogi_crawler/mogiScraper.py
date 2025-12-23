from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from fake_useragent import UserAgent
import undetected_chromedriver as uc
import concurrent.futures
from tenacity import retry, stop_after_attempt, wait_exponential
import pandas as pd
import datetime
import re

current_datetime = datetime.datetime.now()
current_day = current_datetime.day
current_month = current_datetime.month
current_year = current_datetime.year
previous_datetime = current_datetime - datetime.timedelta(days=1)
previous_day = previous_datetime.day
previous_month = previous_datetime.month
previous_year = previous_datetime.year

class MogiCrawler():
    def __init__(self, base_url, num_pages=None, start_page=1):
        self.base_url = base_url
        self.num_pages = num_pages
        self.start_page = start_page
        if self.num_pages:
            print(
                f"Initialized mogi.vn from page {start_page} to "
                f"page {start_page + num_pages - 1} with base URL: {base_url}"
            )
        else:
            print(f"Initialized mogi.vn from page {start_page} with base URL: {base_url}")
    def init_driver(self):
        options = Options()
        user_agent = UserAgent().random
        options.add_argument(f'--user-agent={user_agent}')
        driver = uc.Chrome(version_main=141, options=options)
        driver.implicitly_wait(10)
        actions = ActionChains(driver)
        wait = WebDriverWait(driver, 10)
        return driver, actions, wait
    def get_pages(self, driver):
        page_links = []
        current_page = self.start_page
        try:
            while True:
                if current_page == 1:
                    url = self.base_url
                else:
                    url = self.base_url + '?cp=' + str(current_page) 
                driver.get(url)
                driver.implicitly_wait(0.5)
                # Updated to find <a class="link-overlay"> inside <div class="prop-info">
                links = driver.find_elements(By.XPATH, value="//div[@class='prop-info']/a[@class='link-overlay']")
                # Stop if the page is empty
                if not links:
                    print(f"No more links found at page {current_page}. Stopping.")
                    break 
                for link in links:
                    page_links.append(link.get_attribute('href'))
                current_page += 1
                if self.num_pages and (current_page - self.start_page) >= self.num_pages:
                    print(f'Process ended with total of {self.num_pages} pages')
                    break
        except Exception as error:
            print(f'Process ended with total of {current_page - self.start_page} pages: {error}')
            return page_links
        finally:
            driver.quit()
        return page_links
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def extract(self, page_url): 
        driver, actions, wait = self.init_driver()
        print(f'Extracting from: {page_url}')
        allowed_columns = [
            "Mã BĐS",
            "Ngày đăng",
            "Đường",
            "Phường",
            "Quận",
            "Thành phố",
            "Diện tích",
            "Chiều ngang",
            "Chiều dài",
            "Pháp lý",
            "Số phòng ngủ",
            "Số phòng vệ sinh",
            "Giá"
        ]
        house_data = {column: None for column in allowed_columns}
        seen_keys = set() # avoid overwriting parsed data
        try:
            driver.get(page_url)
            # 1. Property ID (Mã BĐS)
            try:
                property_id = driver.find_element(By.XPATH, "//div[@class='info-attrs']//span[text()='Mã BĐS']/following-sibling::span").text.strip()
                house_data['Mã BĐS'] = str(property_id)
            except:
                try:
                    # Fallback to URL parsing
                    property_id = page_url.split('-id')[-1].split('?')[0]
                    house_data['Mã BĐS'] = str(property_id)
                except:
                    pass 
            seen_keys.add('Mã BĐS')
            # 2. Address (Đường, Phường, Quận, Thành phố)
            try:
                # Parse from the <div class="address">
                address_full = driver.find_element(By.XPATH, "//div[@class='address']").text.strip()
                address_parts = [part.strip() for part in address_full.split(',')]
                # Parse in reverse order
                if len(address_parts) >= 1:
                    house_data['Thành phố'] = address_parts[-1]
                    seen_keys.add('Thành phố')
                if len(address_parts) >= 2:
                    house_data['Quận'] = address_parts[-2]
                    seen_keys.add('Quận')
                if len(address_parts) >= 3:
                    house_data['Phường'] = address_parts[-3]
                    seen_keys.add('Phường')
                if len(address_parts) >= 4:
                    house_data['Đường'] = ", ".join(address_parts[:-3])
                    seen_keys.add('Đường')
            except Exception as e:
                print(f"Could not parse address for {page_url}: {e}")
            # 3. Price (Giá)
            try:
                price_text = driver.find_element(By.XPATH, "//div[@class='price']").text.strip()
                house_data['Giá'] = price_text
                seen_keys.add('Giá')
            except Exception as e:
                print(f"Could not find Giá for {page_url}: {e}")
            # 4. Main Details Table (info-attrs)
            details_list = driver.find_elements(By.XPATH, "//div[@class='info-attrs clearfix']/div[@class='info-attr clearfix']")
            for item in details_list:
                try:
                    key = item.find_element(By.XPATH, "./span[1]").text.strip()
                    value = item.find_element(By.XPATH, "./span[2]").text.strip()
                    if value in ['_', '---', '']:
                        value = None
                        continue
                    # 1. Special parsing for "Diện tích đất"
                    if key == 'Diện tích đất':
                        # Replace commas with dots to handle floats
                        value_text = value.replace(',', '.').replace('m2', '').replace('m²', '').strip()
                        # Regex to find area, width, length (now handles floats)
                        match = re.search(
                            r'([\d\.]+)\s*\(([\d\.]+)\s*m?\s*[x\*]\s*([\d\.]+)', 
                            value_text, 
                            re.IGNORECASE
                        )
                        if match:
                            house_data['Diện tích'] = match.group(1).strip()
                            house_data['Chiều ngang'] = match.group(2).strip()
                            house_data['Chiều dài'] = match.group(3).strip()
                        else:
                            # Try to get Area only
                            match_area = re.search(r'([\d\.]+)', value_text)
                            if match_area:
                                house_data['Diện tích'] = match_area.group(1).strip()
                        seen_keys.update(['Diện tích', 'Chiều ngang', 'Chiều dài'])
                        continue 
                    # 2. Key Mapping for allowed_columns
                    mapped_key = key 
                    if key == 'Phòng ngủ':
                        mapped_key = 'Số phòng ngủ'
                        value = re.sub(r'[^0-9]', '', value).strip() 
                    elif key == 'Nhà tắm':
                        mapped_key = 'Số phòng vệ sinh'
                        value = re.sub(r'[^0-9]', '', value).strip() 
                    elif key == 'Pháp lý':
                        mapped_key = 'Pháp lý'
                    elif key == 'Ngày đăng':
                        mapped_key = 'Ngày đăng'
                    # 3. Add to dict if it's in our list and not already parsed
                    if mapped_key in allowed_columns and mapped_key not in seen_keys:
                        house_data[mapped_key] = value
                        seen_keys.add(mapped_key)     
                except Exception as e:
                    print(f"Error parsing detail item for {page_url}: {e}")
        except Exception as error:
            print(f"Error extracting {page_url}: {error}")
            raise
        finally:
            driver.quit()
        return house_data
    def multithread_extract(self, max_workers=4):
        driver, _, _ = self.init_driver()
        page_links = self.get_pages(driver)
        all_data = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_page = {executor.submit(self.extract, page): page for page in page_links}
            for future in concurrent.futures.as_completed(future_to_page):
                url = future_to_page[future]
                try:
                    data = future.result()
                    if data is not None:
                        all_data.append(data)
                except Exception as error:
                    print(f'Failed to extract: {url} - {error}')
        data_frame = pd.DataFrame(all_data)
        return data_frame
    def load_to_duckdb(self, data_frame, client):
        table_name = 'mogi_listings'
        if data_frame.empty:
            print(f"DataFrame is empty. Skipping load to DuckDB for {table_name}.")
            return
        try:
            client.sql(f"DROP TABLE IF EXISTS {table_name}")
            client.sql(f"CREATE TABLE {table_name} AS SELECT * FROM data_frame LIMIT 0")
            client.sql(f"INSERT INTO {table_name} SELECT * FROM data_frame")
            print(f"Data loaded successfully into DuckDB table: {table_name}")
        except Exception as error:
            print(f"Failed to load data to DuckDB table {table_name}: {error}")
            raise