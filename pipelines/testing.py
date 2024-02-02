import scrapy
import json
from bs4 import BeautifulSoup
import re
import logging
import os
import sys


# Add the parent directory (containing 'spiders') to the sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
access_dir = os.path.join(current_dir, '..') 

db_path = os.path.join(parent_dir, 'database/historical_raw.db')

print(f"db_path: {db_path}")




