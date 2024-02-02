import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import json
import sqlite3
from datetime import datetime
import sys
import os


# Add the parent directory (containing 'spiders') to the sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
access_dir = os.path.join(current_dir, '..')  # Assuming 'spiders' is in the parent directory


sys.path.append(access_dir)
print(sys.path)

from spiders.upcoming_bouts_spider import BoutSpider

class JsonWriterPipeline(object):
    def open_spider(self, spider):
        self.items = []

    def close_spider(self, spider):
        json_data = json.dumps(self.items)
        print(json_data)
        self.insert_into_db(json.loads(json_data))  # Convert JSON string back to list

    def process_item(self, item, spider):
        self.items.append(item)
        return item
    
    def convert_date(self, json_data):
        for item in json_data:
            date_str = item.get('date', '').strip()  # Trim whitespace
            if date_str:
                try:
                    date_obj = datetime.strptime(date_str, '%B %d, %Y')  # Ensure format matches
                    item['date'] = date_obj.strftime('%m-%d-%Y')
                except ValueError as e:
                    print(f"Error parsing date '{date_str}': {e}")
        
    def insert_into_db(self, json_data):
        # Convert date strings to the correct format
        self.convert_date(json_data)
        try:
            db_path = os.path.join(parent_dir, 'database/historical_raw.db')
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            # Create the upcoming_bouts table if it does not exist
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS upcoming_bouts (
                    event_title TEXT,
                    date TEXT,
                    location TEXT,
                    fighter_a_name TEXT,
                    fighter_b_name TEXT,
                    weight_class TEXT,
                    cage_size TEXT
                )
            ''')

            # Clear the table before inserting new data
            cursor.execute("DELETE FROM upcoming_bouts")

            for item in json_data:
                columns = ', '.join(item.keys())
                placeholders = ', '.join('?' * len(item))
                sql = f"INSERT INTO upcoming_bouts ({columns}) VALUES ({placeholders})"
                cursor.execute(sql, list(item.values()))

            conn.commit()
        except sqlite3.Error as e:
            print(f"SQLite error: {e}")
        finally:
            conn.close()

def run_spider():
    settings = get_project_settings()
    settings['ITEM_PIPELINES'] = {'__main__.JsonWriterPipeline': 1}
    
    process = CrawlerProcess(settings)
    process.crawl(BoutSpider)
    process.start()

if __name__ == "__main__":
    run_spider()
