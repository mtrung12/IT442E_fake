from mogiScraper import MogiCrawler
import pandas as pd
import yaml
import duckdb  

def get_config(config_path):
    with open(config_path, 'r', encoding='utf8') as file:
        return yaml.safe_load(file)
def update_config(updated_config, config_path):
    try:
        with open(config_path, 'w', encoding='utf8') as file:
            yaml.dump(updated_config, file, allow_unicode=True)
        print("Configuration file updated successfully!")
    except Exception as error:
        print(f"Configuration file could not be updated: {error}")
def run_crawler(config_path='config.yaml'):
    try:
        db_file = 'real_estate.db'
        with duckdb.connect(database=db_file, read_only=False) as client:
            config = get_config(config_path)
            updated_config = config
            crawler_key = 'mogiCrawler'
            final_data_frame = pd.DataFrame(columns=config[crawler_key]['final_columns'])
            try:
                scraper = MogiCrawler(
                    num_pages=config[crawler_key]['num_pages'],
                    base_url=config[crawler_key]['base_url'],
                    start_page=config[crawler_key]['start_page']
                )
                data_frame = scraper.multithread_extract(max_workers=1)
                final_data_frame = pd.concat([final_data_frame, data_frame], ignore_index=True)
                scraper.load_to_duckdb(final_data_frame, client)
                updated_config[crawler_key]['start_page'] += updated_config[crawler_key]['num_pages']
                print(f'Completed scraping {crawler_key}')
                print(f'{crawler_key} data successfully pushed to database')
            except Exception as error:
                print(f"Error occurred while scraping: {str(error)}")
    except Exception as error:
        print(f"An error occurred: {str(error)}")
def main():
    run_crawler()
if __name__ == "__main__":
    main()