import scrapy
import json
from bs4 import BeautifulSoup
import re
import logging
import os


# Get the directory of the current script
current_dir = os.path.dirname(os.path.abspath(__file__))

# Build the absolute path to the log file
log_file_path = os.path.join(current_dir, 'logs', 'upcoming_bouts_spider.log')

class BoutSpider(scrapy.Spider):
    name = "upcoming_bouts_spider"
    allowed_domains = ["ufcstats.com"]
    # Set up logging
    custom_settings = {
        'LOG_FILE': 'upcoming_bouts_spider.log',  # Specify the log file path
        'LOG_LEVEL': 'DEBUG',  # Set the log level to DEBUG
    }


    def start_requests(self):
        url = "http://ufcstats.com/statistics/events/upcoming"
        yield scrapy.Request(url, callback=self.get_upcoming_events)

    def get_upcoming_events(self, response):
        upcoming_event_urls = response.css('tr.b-statistics__table-row a::attr(href)').getall()
        for url in upcoming_event_urls:
            yield scrapy.Request(url, callback=self.get_bouts)

    def get_bouts(self, response):
        bouts = response.css('tr.b-fight-details__table-row').getall()
        for bout in range(1, len(bouts) + 1):
            event_title = response.css('span.b-content__title-highlight::text').get().strip()
            date = response.xpath('string(//li[@class="b-list__box-list-item"]/i[@class="b-list__box-item-title"][contains(text(), "Date:")]/following-sibling::text()[1])').get().strip()
            location = response.xpath('string(//li[@class="b-list__box-list-item"]/i[@class="b-list__box-item-title"][contains(text(), "Location:")]/following-sibling::text()[1])').get().strip()
            fighter_a_name = response.css(f'tr.b-fight-details__table-row:nth-child({bout}) td.b-fight-details__table-col.l-page_align_left:nth-child(2) p.b-fight-details__table-text:nth-child(1) a.b-link::text').get().strip()
            fighter_b_name = response.css(f'tr.b-fight-details__table-row:nth-child({bout}) td.b-fight-details__table-col.l-page_align_left:nth-child(2) p.b-fight-details__table-text:nth-child(2) a.b-link::text').get().strip()
            # response.css('tr.b-fight-details__table-row:nth-child(1) td.b-fight-details__table-col.l-page_align_left:nth-child(2) p.b-fight-details__table-text:nth-child(2) a.b-link::text').get().strip()
            weight_class = response.css(f'tr.b-fight-details__table-row:nth-child({bout}) td.b-fight-details__table-col:nth-child(7) p.b-fight-details__table-text::text').get().strip()
            #response.css('tr.b-fight-details__table-row:nth-child(1) td.b-fight-details__table-col:nth-child(7) p.b-fight-details__table-text::text').get().strip()
            
            #if event_title does not contain 'Fight Night' and location does not contain 'Vegas':
            if 'Fight Night' in event_title and 'Vegas' in location:
                cage_size = 'small'
            else:
                cage_size = 'big'



            yield {
                'event_title': event_title,
                'date': date,
                'location': location,
                'fighter_a_name': fighter_a_name,
                'fighter_b_name': fighter_b_name,
                'weight_class': weight_class,
                'cage_size': cage_size,
            }
