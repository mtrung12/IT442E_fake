import yaml
from nhatot_crawler import NhatotCrawler
from export_to_csv import export_to_csv
import os
from logger import log
import pandas as pd

def main():
    with open('config.yaml', 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    crawler = NhatotCrawler(config)
    crawler.scrape_and_save()
    export_to_csv(config['db_file'], 'ads', 'nhatot_export.csv')
    
    # if need to add more features without running crawler from the beginning
    # update the column_mapping in config.yaml and run below code
    
    # df = pd.read_csv('nhatot_export.csv')
    # id_list = df['id'].tolist()
    # crawler.refresh_ads(id_list)
    
if __name__ == "__main__":
    main()