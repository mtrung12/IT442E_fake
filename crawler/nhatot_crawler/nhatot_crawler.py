import duckdb
from WebCrawler import WebCrawler
import requests
import json
import time
import datetime
import yaml
from logger import log

class NhatotCrawler:
    def __init__(self, config):
        self.config = config
        if 'base_url' in config and config['base_url']:
            base_url = config['base_url']
        elif 'base_url_template' in config and config['base_url_template']:
            first_city = config.get('cities')[0] if config.get('cities') else 'tp-ho-chi-minh'
            base_url = config['base_url_template'].format(city=first_city)
        else:
            raise KeyError("config must contain 'base_url' or 'base_url_template'")
        self.crawler = WebCrawler(base_url, config['user_agent'])
        self.db_file = config['db_file']
        self.gateway_base = config['gateway_base']
        self.column_mapping = config.get('column_mapping', {}) 
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': config['user_agent']})
        self.con = duckdb.connect(self.db_file)  # Reuse connection
        self._init_db()
        self.batch = [] 
        self.batch_size = config.get('batch_size', 100)  

    def _init_db(self):
        columns_def = []
        for key_path, col_name in self.column_mapping.items():
            if col_name == "id":
                continue
            col_type = self._get_column_type(key_path)
            columns_def.append(f'"{col_name}" {col_type}')
        
        mapped_values = set(self.column_mapping.values())
        if "timestamp" not in mapped_values:
            columns_def.append(f'"timestamp" TIMESTAMP')
        if "post_date" not in mapped_values:
            columns_def.append(f'"post_date" DATE')
        
        create_sql = f"""
            CREATE TABLE IF NOT EXISTS ads (
                "id" INTEGER PRIMARY KEY,
                {', '.join(columns_def) if columns_def else ''}
            )
        """
        self.con.execute(create_sql)

    def _get_column_type(self, key_path):
        double_list = ['ad.width', 'ad.length', 'ad.price_million_per_m2', 'ad.price', 'ad.size']
        integer_list = ['ad.list_id', 'ad.rooms', 'ad.toilets']
        if key_path in integer_list:
            return 'INTEGER'
        elif 'is_main_street' in key_path:
            return 'BOOLEAN'
        elif key_path in double_list:
            return 'DOUBLE'
        return 'VARCHAR'

    def _extract_value(self, data, key_path):
        if key_path.startswith('special:'):
            special_key = key_path.split(':', 1)[1]
            if special_key == 'latitude_longitude':
                lat = self._extract_value(data, 'ad.latitude')
                lon = self._extract_value(data, 'ad.longitude')
                return f"{lat},{lon}" if lat and lon else None
            return None
        
        keys = key_path.split('.')
        value = data
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return None
        return value

    def fetch_ad_json(self, ad_id):
        url = f"{self.gateway_base}{ad_id}"
        try:
            response = self.session.get(url)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            log.info(f"Error fetching ad {ad_id}: {e}")
            return None

    def save_to_db(self, ad_id, data):
        extracted = {}
        extracted["id"] = data.get('ad', {}).get('list_id', ad_id)
        
        for key_path, col_name in self.column_mapping.items():
            value = self._extract_value(data, key_path)
            extracted[col_name] = value
        
        raw_ts = extracted.get('unix_timestamp')
        if raw_ts is not None:
            try:
                ts = int(raw_ts)
                if ts > 10**11:
                    ts = ts // 1000
                extracted['unix_timestamp'] = ts
            except Exception:
                extracted['unix_timestamp'] = None
        
        unix_ts = extracted.get('unix_timestamp')
        extracted["timestamp"], extracted["post_date"] = self._convert_unix_timestamp(unix_ts)
        
        self.batch.append(extracted)
        
        if len(self.batch) >= self.batch_size:
            self.flush_batch()

    def flush_batch(self):
        if not self.batch:
            return
        
        columns = list(self.batch[0].keys()) 
        values_list = [tuple(d[col] for col in columns) for d in self.batch]
        
        columns_str = ', '.join(f'"{k}"' for k in columns)
        placeholders = ', '.join('?' for _ in columns)
        update_set = ', '.join(f'"{k}" = EXCLUDED."{k}"' for k in columns if k != "id")
        
        upsert_sql = f"""
            INSERT INTO ads ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT("id") DO UPDATE SET {update_set}
        """
        
        self.con.executemany(upsert_sql, values_list)
        self.batch = []  
        log.info("Flushed batch to DB (size: {})", len(values_list))

    def scrape_and_save(self):
        start_page = self.config.get('start_page', 1)
        max_pages = self.config['max_pages']
        base_template = self.config.get('base_url_template', self.config.get('base_url', ''))
        cities = self.config.get('cities')
        if not cities:
            last = self.config.get('base_url', '').rstrip('/').split('-')[-1]
            cities = [last] if last else ['ha-noi']
        total_saved = 0
        for city in cities:
            if '{city}' in base_template:
                city_base = base_template.format(city=city)
            else:
                city_base = base_template.replace('ha-noi', city)
            self.crawler = WebCrawler(city_base, self.config['user_agent'])
            log.info("=== Crawling city {} ===", city)
            for page in range(start_page, start_page + max_pages):
                log.info("=== Crawling Page {} ===", page)
                ad_ids = self.crawler.crawl_page(page)
                if not ad_ids:
                    log.warning("No ads found on page {} for city {} – stopping city.", page, city)
                    break
                for i, ad_id in enumerate(ad_ids, 1):
                    if self._is_already_saved(ad_id):
                        log.info("Skip ad {} (already in DB)", ad_id)
                        continue
                    data = self.fetch_ad_json(ad_id)
                    if data:
                        self.save_to_db(ad_id, data)
                        log.info("Queued ad {} for save ({}/{})", ad_id, i, len(ad_ids))
                        total_saved += 1
                    else:
                        log.error("Failed to fetch JSON for ad {}", ad_id)
                    time.sleep(2)
                self.flush_batch() 
                log.info("Finished page {} for city {}. Total saved so far: {}", page, city, total_saved)
        self.flush_batch() 
        self.con.close()  
        log.info("All cities finished. Total saved: {}", total_saved)
            
    def _is_already_saved(self, ad_id):
        result = self.con.execute(
            'SELECT 1 FROM ads WHERE "id" = ?', (ad_id,)
        ).fetchone()
        return result is not None
    
    
    def _convert_unix_timestamp(self, unix_ts):
        if unix_ts is None:
            return None, None
        try:
            ts = int(unix_ts)
            ts_seconds = ts // 1000 if ts > 10**11 else ts
            dt = datetime.datetime.fromtimestamp(ts_seconds) 
            return dt.strftime("%Y-%m-%d %H:%M:%S"), dt.date().isoformat()
        except Exception:
            return None, None
    
    def refresh_ads(self, ad_id_list, sleep_time=1):
        total = len(ad_id_list)
        log.info("=== Refreshing {} ads ===", total)
        for idx, ad_id in enumerate(ad_id_list, 1):
            log.info("Refreshing ad {} ({}/{})", ad_id, idx, total)
            data = self.fetch_ad_json(ad_id)
            if data:
                self.save_to_db(ad_id, data)
                log.info("Queued ad {} for update", ad_id)
            else:
                log.error("Failed to fetch JSON for ad {}", ad_id)
            time.sleep(sleep_time)
        self.flush_batch()  
        self.con.close() 
        log.info("=== Refresh complete. Updated {} ads ===", total)