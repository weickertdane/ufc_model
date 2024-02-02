import scrapy
import json
from bs4 import BeautifulSoup
import re
import logging
import os


# Get the directory of the current script
current_dir = os.path.dirname(os.path.abspath(__file__))

# Build the absolute path to the log file
log_file_path = os.path.join(current_dir, 'logs', 'get_recent_event_spider.log')



class BoutSpider(scrapy.Spider):
    name = "latest_bouts_spider"
    allowed_domains = ["ufcstats.com"]
    # Set up logging
    custom_settings = {
        'LOG_FILE': log_file_path,  # Specify the log file path
        'LOG_LEVEL': 'DEBUG',  # Set the log level to DEBUG
    }


    def start_requests(self):
        url = "http://ufcstats.com/statistics/events/completed?page=all"
        yield scrapy.Request(url, callback=self.get_latest_event)

    def get_latest_event(self, response):
        latest_event_url = response.css('tr.b-statistics__table-row:nth-child(3) a::attr(href)').get() # 3, gets most recent, 4 gets the second most recent
        if latest_event_url:
            yield scrapy.Request(latest_event_url, callback=self.get_bout_links)

    def get_bout_links(self, response):
        bout_urls = response.css('tr.b-fight-details__table-row::attr(data-link)').getall()
        date = response.xpath('string(//li[@class="b-list__box-list-item"]/i[@class="b-list__box-item-title"][contains(text(), "Date:")]/following-sibling::text()[1])').get()
        location = response.xpath('string(//li[@class="b-list__box-list-item"]/i[@class="b-list__box-item-title"][contains(text(), "Location:")]/following-sibling::text()[1])').get()

        for bout_url in bout_urls:
            yield scrapy.Request(bout_url, callback=self.parse_bouts, meta={'date': date, 'location': location})

    def parse_bouts(self, response):
        event_link = response.css('a.b-link::attr(href)').get()
        # Retrieve date and location from the meta attribute
        date = response.meta['date']
        location = response.meta['location']
        event_title = response.css('h2.b-content__title a.b-link::text').get()
        fighter_a_name = response.css('div.b-fight-details__person:nth-child(1) h3.b-fight-details__person-name a::text').get()
        fighter_b_name = response.css('div.b-fight-details__person:nth-child(2) h3.b-fight-details__person-name a::text').get()
        #fighter_a_nickname = response.css('div.b-fight-details__person:nth-child(1) p.b-fight-details__person-title::text').get()
        #fighter_b_nickname = response.css('div.b-fight-details__person:nth-child(2) p.b-fight-details__person-title::text').get()
        fighter_a_result = response.css('div.b-fight-details__person:nth-child(1) i.b-fight-details__person-status::text').get()
        fighter_b_result = response.css('div.b-fight-details__person:nth-child(2) i.b-fight-details__person-status::text').get()

        weight_class_with_tag = response.xpath('string(//i[@class="b-fight-details__fight-title"]/text()[2])').get()
        #logging.debug(f'weight_class_with_tag: {weight_class_with_tag}')
        weight_class_no_tag = response.css('i.b-fight-details__fight-title::text').get()
        method = response.css('div.b-fight-details__content p.b-fight-details__text i.b-fight-details__label:contains("Method:") + i::text').get()
        round_info = response.css('div.b-fight-details__content p.b-fight-details__text i:contains("Round:")').get()
        #time_text = response.css('div.b-fight-details__content p.b-fight-details__text i:contains("Time:")').get()
        #time_format_text = response.css('div.b-fight-details__content p.b-fight-details__text i:contains("Time format:")').get()
        # Extract the entire text block containing 'Time:' and 'Time format:' and process it
        time_block = response.css('div.b-fight-details__content p.b-fight-details__text i:contains("Time:")').get()
        time_format_block = response.css('div.b-fight-details__content p.b-fight-details__text i:contains("Time format:")').get()
        referee_text = response.css('i.b-fight-details__text-item:contains("Referee:") span::text').get()
        detail_description = response.css('div.b-fight-details__content p.b-fight-details__text:nth-of-type(2)').get()

        
        judge_a_name = response.css('p.b-fight-details__text:nth-of-type(2) i.b-fight-details__text-item:nth-of-type(2) span:nth-of-type(1)::text').get()
        judge_b_name = response.css('p.b-fight-details__text:nth-of-type(2) i.b-fight-details__text-item:nth-of-type(3) span:nth-of-type(1)::text').get()
        judge_c_name = response.css('p.b-fight-details__text:nth-of-type(2) i.b-fight-details__text-item:nth-of-type(4) span:nth-of-type(1)::text').get()
        contains_judge_scores = response.css('p.b-fight-details__text i.b-fight-details__text-item').getall()

        # Extract and format the scores
        for i, element in enumerate(contains_judge_scores[-3:]):
            selector = scrapy.Selector(text=element)  # Convert the string to a Scrapy Selector
            text = selector.css('::text').getall()  # Get all text content within the element
            cleaned_text = ' '.join(text).replace('\n', '').strip()  # Remove newlines and leading/trailing whitespace
                    
            # Split the cleaned text by the second whitespace
            parts = cleaned_text.split(' ', 2)
                    
            # Assign values to variables based on index
            if i == 0:
                judge_a_score = parts[-1].strip('.').strip()
            elif i == 1:
                judge_b_score = parts[-1].strip('.').strip()
            elif i == 2:
                judge_c_score = parts[-1].strip('.').strip()  # Corrected variable name



        #Knockdowns - column 2, use as non-split template

        fighter_a_knockdowns = []
        fighter_b_knockdowns = []
        fighter_a_kd_dict = {}
        fighter_b_kd_dict = {}

        for i in range(1, 6):
            fighter_a_knockdown = response.css(f'table.b-fight-details__table tbody:nth-of-type(1) tr.b-fight-details__table-row:nth-of-type({i}) td.b-fight-details__table-col:nth-of-type(2) p.b-fight-details__table-text:nth-child(1)::text').get()
            
            if fighter_a_knockdown is not None:
                fighter_a_knockdown = fighter_a_knockdown.strip()  # Strip the value if it's not None
            
            fighter_a_knockdowns.append(fighter_a_knockdown)

            for i, fighter_a_knockdown in enumerate(fighter_a_knockdowns, start=1):
                globals()[f'fighter_a_rd_{i}_kd'] = fighter_a_knockdown
                fighter_a_kd_dict[f'fighter_a_rd_{i}_kd'] = fighter_a_knockdown  # Add to the dictionary


        for i in range(1, 6):
            fighter_b_knockdown = response.css(f'table.b-fight-details__table tbody:nth-of-type(1) tr.b-fight-details__table-row:nth-of-type({i}) td.b-fight-details__table-col:nth-of-type(2) p.b-fight-details__table-text:nth-child(2)::text').get()
            
            if fighter_b_knockdown is not None:
                fighter_b_knockdown = fighter_b_knockdown.strip()  # Strip the value if it's not None

            fighter_b_knockdowns.append(fighter_b_knockdown)

            for i, fighter_b_knockdown in enumerate(fighter_b_knockdowns, start=1):
                globals()[f'fighter_b_rd_{i}_kd'] = fighter_b_knockdown
                fighter_b_kd_dict[f'fighter_b_rd_{i}_kd'] = fighter_b_knockdown  # Add to the dictionary


        #Sig Strikes - column 3, use as split template
        
        fighter_a_sig_strikes_landed_dict = {}
        fighter_a_sig_strikes_attempted_dict = {}
        fighter_b_sig_strikes_landed_dict = {}
        fighter_b_sig_strikes_attempted_dict = {}

        # Process fighter A's significant strikes
        for i in range(1, 6):
            fighter_a_sig_strike = response.css(f'table.b-fight-details__table tbody:nth-of-type(1) tr.b-fight-details__table-row:nth-of-type({i}) td.b-fight-details__table-col:nth-of-type(3) p.b-fight-details__table-text:nth-child(1)::text').get()
            
            if fighter_a_sig_strike is not None:
                fighter_a_sig_strike = fighter_a_sig_strike.strip()
                fighter_a_sig_strike_landed, fighter_a_sig_strike_attempted = fighter_a_sig_strike.split('of')
                fighter_a_sig_strikes_landed_dict[f'fighter_a_rd_{i}_sig_str_landed'] = fighter_a_sig_strike_landed.strip()
                logging.debug(f'fighter_a_sig_strikes_landed_dict: {fighter_a_sig_strikes_landed_dict}')
                fighter_a_sig_strikes_attempted_dict[f'fighter_a_rd_{i}_sig_str_attempted'] = fighter_a_sig_strike_attempted.strip()
                logging.debug(f'fighter_a_sig_strikes_attempted_dict: {fighter_a_sig_strikes_attempted_dict}')

        

        # Process fighter B's significant strikes
        for i in range(1, 6):
            fighter_b_sig_strike = response.css(f'table.b-fight-details__table tbody:nth-of-type(1) tr.b-fight-details__table-row:nth-of-type({i}) td.b-fight-details__table-col:nth-of-type(3) p.b-fight-details__table-text:nth-child(2)::text').get()
            
            if fighter_b_sig_strike is not None:
                fighter_b_sig_strike = fighter_b_sig_strike.strip()
                fighter_b_sig_strike_landed, fighter_b_sig_strike_attempted = fighter_b_sig_strike.split('of')
                fighter_b_sig_strikes_landed_dict[f'fighter_b_rd_{i}_sig_str_landed'] = fighter_b_sig_strike_landed.strip()
                fighter_b_sig_strikes_attempted_dict[f'fighter_b_rd_{i}_sig_str_attempted'] = fighter_b_sig_strike_attempted.strip()

        # Total Strikes - column 4
                
        fighter_a_total_strikes_landed_dict = {}
        fighter_a_total_strikes_attempted_dict = {}
        fighter_b_total_strikes_landed_dict = {}
        fighter_b_total_strikes_attempted_dict = {}
            
        for i in range(1, 6):
            fighter_a_total_strike = response.css(f'table.b-fight-details__table tbody:nth-of-type(1) tr.b-fight-details__table-row:nth-of-type({i}) td.b-fight-details__table-col:nth-of-type(5) p.b-fight-details__table-text:nth-child(1)::text').get()

            if fighter_a_total_strike is not None:
                fighter_a_total_strike = fighter_a_total_strike.strip()
                fighter_a_total_strike_landed, fighter_a_total_strike_attempted = fighter_a_total_strike.split('of')
                fighter_a_total_strikes_landed_dict[f'fighter_a_rd_{i}_total_str_landed'] = fighter_a_total_strike_landed.strip()
                fighter_a_total_strikes_attempted_dict[f'fighter_a_rd_{i}_total_str_attempted'] = fighter_a_total_strike_attempted.strip()

        #Fighter B Total Strikes
        
        for i in range (1, 6):
            fighter_b_total_strike = response.css(f'table.b-fight-details__table tbody:nth-of-type(1) tr.b-fight-details__table-row:nth-of-type({i}) td.b-fight-details__table-col:nth-of-type(5) p.b-fight-details__table-text:nth-child(2)::text').get()

            if fighter_b_total_strike is not None:
                fighter_b_total_strike = fighter_b_total_strike.strip()
                fighter_b_total_strike_landed, fighter_b_total_strike_attempted = fighter_b_total_strike.split('of')
                fighter_b_total_strikes_landed_dict[f'fighter_b_rd_{i}_total_str_landed'] = fighter_b_total_strike_landed.strip()
                fighter_b_total_strikes_attempted_dict[f'fighter_b_rd_{i}_total_str_attempted'] = fighter_b_total_strike_attempted.strip()

        
        #Takedowns - column 5
        
        fighter_a_takedowns_landed_dict = {}
        fighter_a_takedowns_attempted_dict = {}
        fighter_b_takedowns_landed_dict = {}
        fighter_b_takedowns_attempted_dict = {}

        for i in range(1, 6):
            fighter_a_takedowns = response.css(f'table.b-fight-details__table tbody:nth-of-type(1) tr.b-fight-details__table-row:nth-of-type({i}) td.b-fight-details__table-col:nth-of-type(6) p.b-fight-details__table-text:nth-child(1)::text').get()

            if fighter_a_takedowns is not None:
                fighter_a_takedowns = fighter_a_takedowns.strip()
                fighter_a_takedowns_landed, fighter_a_takedowns_attempted = fighter_a_takedowns.split('of')
                fighter_a_takedowns_landed_dict[f'fighter_a_rd_{i}_takedowns_landed'] = fighter_a_takedowns_landed.strip()
                fighter_a_takedowns_attempted_dict[f'fighter_a_rd_{i}_takedowns_attempted'] = fighter_a_takedowns_attempted.strip()

        #Fighter B Takedowns
                
        for i in range(1, 6):
            fighter_b_takedowns = response.css(f'table.b-fight-details__table tbody:nth-of-type(1) tr.b-fight-details__table-row:nth-of-type({i}) td.b-fight-details__table-col:nth-of-type(6) p.b-fight-details__table-text:nth-child(2)::text').get()

            if fighter_b_takedowns is not None:
                fighter_b_takedowns = fighter_b_takedowns.strip()
                fighter_b_takedowns_landed, fighter_b_takedowns_attempted = fighter_b_takedowns.split('of')
                fighter_b_takedowns_landed_dict[f'fighter_b_rd_{i}_takedowns_landed'] = fighter_b_takedowns_landed.strip()
                fighter_b_takedowns_attempted_dict[f'fighter_b_rd_{i}_takedowns_attempted'] = fighter_b_takedowns_attempted.strip()

        
        # Sub Attempts - column 7
        
        fighter_a_sub_attempts = []
        fighter_b_sub_attempts = []
        fighter_a_sub_attempts_dict = {}
        fighter_b_sub_attempts_dict = {}

        #Fighter A Sub Attempts
        for i in range(1, 6):
            fighter_a_sub_attempt = response.css(f'table.b-fight-details__table tbody:nth-of-type(1) tr.b-fight-details__table-row:nth-of-type({i}) td.b-fight-details__table-col:nth-of-type(8) p.b-fight-details__table-text:nth-child(1)::text').get()
            
            if fighter_a_sub_attempt is not None:
                fighter_a_sub_attempt = fighter_a_sub_attempt.strip()  # Strip the value if it's not None
            
            fighter_a_sub_attempts.append(fighter_a_sub_attempt)

            for i, fighter_a_sub_attempt in enumerate(fighter_a_sub_attempts, start=1):
                globals()[f'fighter_a_rd_{i}_sub_attempts'] = fighter_a_sub_attempt
                fighter_a_sub_attempts_dict[f'fighter_a_rd_{i}_sub_attempts'] = fighter_a_sub_attempt  # Add to the dictionary

        
        #Fighter B Sub Attempts
        for i in range(1, 6):
            fighter_b_sub_attempt = response.css(f'table.b-fight-details__table tbody:nth-of-type(1) tr.b-fight-details__table-row:nth-of-type({i}) td.b-fight-details__table-col:nth-of-type(8) p.b-fight-details__table-text:nth-child(2)::text').get()
            
            if fighter_b_sub_attempt is not None:
                fighter_b_sub_attempt = fighter_b_sub_attempt.strip()

            fighter_b_sub_attempts.append(fighter_b_sub_attempt)

            for i, fighter_b_sub_attempt in enumerate(fighter_b_sub_attempts, start=1):
                globals()[f'fighter_b_rd_{i}_sub_attempts'] = fighter_b_sub_attempt
                fighter_b_sub_attempts_dict[f'fighter_b_rd_{i}_sub_attempts'] = fighter_b_sub_attempt

        
        #Reversals - column 8
                
        fighter_a_reversals = []
        fighter_b_reversals = []
        fighter_a_reversals_dict = {}
        fighter_b_reversals_dict = {}

        for i in range(1, 6):
            fighter_a_reversal = response.css(f'table.b-fight-details__table tbody:nth-of-type(1) tr.b-fight-details__table-row:nth-of-type({i}) td.b-fight-details__table-col:nth-of-type(9) p.b-fight-details__table-text:nth-child(1)::text').get()

            if fighter_a_reversal is not None:
                fighter_a_reversal = fighter_a_reversal.strip()
            
            fighter_a_reversals.append(fighter_a_reversal)

            for i, fighter_a_reversal in enumerate(fighter_a_reversals, start=1):
                globals()[f'fighter_a_rd_{i}_reversals'] = fighter_a_reversal
                fighter_a_reversals_dict[f'fighter_a_rd_{i}_reversals'] = fighter_a_reversal

        #Fighter B Reversals
                
        for i in range(1, 6):
            fighter_b_reversal = response.css(f'table.b-fight-details__table tbody:nth-of-type(1) tr.b-fight-details__table-row:nth-of-type({i}) td.b-fight-details__table-col:nth-of-type(9) p.b-fight-details__table-text:nth-child(2)::text').get()

            if fighter_b_reversal is not None:
                fighter_b_reversal = fighter_b_reversal.strip()
            
            fighter_b_reversals.append(fighter_b_reversal)

            for i, fighter_b_reversal in enumerate(fighter_b_reversals, start=1):
                globals()[f'fighter_b_rd_{i}_reversals'] = fighter_b_reversal
                fighter_b_reversals_dict[f'fighter_b_rd_{i}_reversals'] = fighter_b_reversal


        # Control - column 9
        
        fighter_a_controls = []
        fighter_b_controls = []
        fighter_a_control_dict = {}
        fighter_b_control_dict = {}

        #Fighter A Control
        for i in range(1, 6):
            fighter_a_control = response.css(f'table.b-fight-details__table tbody:nth-of-type(1) tr.b-fight-details__table-row:nth-of-type({i}) td.b-fight-details__table-col:nth-of-type(10) p.b-fight-details__table-text:nth-child(1)::text').get()

            if fighter_a_control is not None:
                fighter_a_control = fighter_a_control.strip()
            
            fighter_a_controls.append(fighter_a_control)

            for i, fighter_a_control in enumerate(fighter_a_controls, start=1):
                globals()[f'fighter_a_rd_{i}_control'] = fighter_a_control
                fighter_a_control_dict[f'fighter_a_rd_{i}_control'] = fighter_a_control

        #Fighter B Control
        for i in range(1, 6):
            fighter_b_control = response.css(f'table.b-fight-details__table tbody:nth-of-type(1) tr.b-fight-details__table-row:nth-of-type({i}) td.b-fight-details__table-col:nth-of-type(10) p.b-fight-details__table-text:nth-child(2)::text').get()

            if fighter_b_control is not None:
                fighter_b_control = fighter_b_control.strip()

            fighter_b_controls.append(fighter_b_control)

            for i, fighter_b_control in enumerate(fighter_b_controls, start=1):
                globals()[f'fighter_b_rd_{i}_control'] = fighter_b_control
                fighter_b_control_dict[f'fighter_b_rd_{i}_control'] = fighter_b_control

            
        # Head Strikes - table 2 column 4
        fighter_a_head_strikes_landed_dict = {}
        fighter_a_head_strikes_attempted_dict = {}
        fighter_b_head_strikes_landed_dict = {}
        fighter_b_head_strikes_attempted_dict = {}

        for i in range(1,6):
            fighter_a_head_strikes = response.css(f'section.b-fight-details__section:nth-of-type(5) table.b-fight-details__table:nth-of-type(1) tbody:nth-of-type(1) tr.b-fight-details__table-row:nth-of-type({i}) td.b-fight-details__table-col:nth-of-type(4) p.b-fight-details__table-text:nth-child(1)::text').get()

            if fighter_a_head_strikes is not None:
                fighter_a_head_strikes = fighter_a_head_strikes.strip()
                fighter_a_head_strikes_landed, fighter_a_head_strikes_attempted = fighter_a_head_strikes.split('of')
                fighter_a_head_strikes_landed_dict[f'fighter_a_rd_{i}_head_strikes_landed'] = fighter_a_head_strikes_landed.strip()
                fighter_a_head_strikes_attempted_dict[f'fighter_a_rd_{i}_head_strikes_attempted'] = fighter_a_head_strikes_attempted.strip()
        
        #Fighter B Head Strikes
        for i in range(1,6):
            fighter_b_head_strikes = response.css(f'section.b-fight-details__section:nth-of-type(5) table.b-fight-details__table:nth-of-type(1) tbody:nth-of-type(1) tr.b-fight-details__table-row:nth-of-type({i}) td.b-fight-details__table-col:nth-of-type(4) p.b-fight-details__table-text:nth-child(2)::text').get()

            if fighter_b_head_strikes is not None:
                fighter_b_head_strikes = fighter_b_head_strikes.strip()
                fighter_b_head_strikes_landed, fighter_b_head_strikes_attempted = fighter_b_head_strikes.split('of')
                fighter_b_head_strikes_landed_dict[f'fighter_b_rd_{i}_head_strikes_landed'] = fighter_b_head_strikes_landed.strip()
                fighter_b_head_strikes_attempted_dict[f'fighter_b_rd_{i}_head_strikes_attempted'] = fighter_b_head_strikes_attempted.strip()


        # Body Strikes - table 2 column 5
        fighter_a_body_strikes_landed_dict = {}
        fighter_a_body_strikes_attempted_dict = {}
        fighter_b_body_strikes_landed_dict = {}
        fighter_b_body_strikes_attempted_dict = {}

        for i in range(1,6):
            fighter_a_body_strikes = response.css(f'section.b-fight-details__section:nth-of-type(5) table.b-fight-details__table:nth-of-type(1) tbody:nth-of-type(1) tr.b-fight-details__table-row:nth-of-type({i}) td.b-fight-details__table-col:nth-of-type(5) p.b-fight-details__table-text:nth-child(1)::text').get()

            if fighter_a_body_strikes is not None:
                fighter_a_body_strikes = fighter_a_body_strikes.strip()
                fighter_a_body_strikes_landed, fighter_a_body_strikes_attempted = fighter_a_body_strikes.split('of')
                fighter_a_body_strikes_landed_dict[f'fighter_a_rd_{i}_body_strikes_landed'] = fighter_a_body_strikes_landed.strip()
                fighter_a_body_strikes_attempted_dict[f'fighter_a_rd_{i}_body_strikes_attempted'] = fighter_a_body_strikes_attempted.strip()
        
        #Fighter B Body Strikes
        for i in range(1,6):
            fighter_b_body_strikes = response.css(f'section.b-fight-details__section:nth-of-type(5) table.b-fight-details__table:nth-of-type(1) tbody:nth-of-type(1) tr.b-fight-details__table-row:nth-of-type({i}) td.b-fight-details__table-col:nth-of-type(5) p.b-fight-details__table-text:nth-child(2)::text').get()

            if fighter_b_body_strikes is not None:
                fighter_b_body_strikes = fighter_b_body_strikes.strip()
                fighter_b_body_strikes_landed, fighter_b_body_strikes_attempted = fighter_b_body_strikes.split('of')
                fighter_b_body_strikes_landed_dict[f'fighter_b_rd_{i}_body_strikes_landed'] = fighter_b_body_strikes_landed.strip()
                fighter_b_body_strikes_attempted_dict[f'fighter_b_rd_{i}_body_strikes_attempted'] = fighter_b_body_strikes_attempted.strip()
        


        # Leg Strikes - table 2 column 6
                
        fighter_a_leg_strikes_landed_dict = {}
        fighter_a_leg_strikes_attempted_dict = {}
        fighter_b_leg_strikes_landed_dict = {}
        fighter_b_leg_strikes_attempted_dict = {}

        for i in range(1,6):
            fighter_a_leg_strikes = response.css(f'section.b-fight-details__section:nth-of-type(5) table.b-fight-details__table:nth-of-type(1) tbody:nth-of-type(1) tr.b-fight-details__table-row:nth-of-type({i}) td.b-fight-details__table-col:nth-of-type(6) p.b-fight-details__table-text:nth-child(1)::text').get()

            if fighter_a_leg_strikes is not None:
                fighter_a_leg_strikes = fighter_a_leg_strikes.strip()
                fighter_a_leg_strikes_landed, fighter_a_leg_strikes_attempted = fighter_a_leg_strikes.split('of')
                fighter_a_leg_strikes_landed_dict[f'fighter_a_rd_{i}_leg_strikes_landed'] = fighter_a_leg_strikes_landed.strip()
                fighter_a_leg_strikes_attempted_dict[f'fighter_a_rd_{i}_leg_strikes_attempted'] = fighter_a_leg_strikes_attempted.strip()

        #Fighter B Leg Strikes
        for i in range(1,6):
            fighter_b_leg_strikes = response.css(f'section.b-fight-details__section:nth-of-type(5) table.b-fight-details__table:nth-of-type(1) tbody:nth-of-type(1) tr.b-fight-details__table-row:nth-of-type({i}) td.b-fight-details__table-col:nth-of-type(6) p.b-fight-details__table-text:nth-child(2)::text').get()

            if fighter_b_leg_strikes is not None:
                fighter_b_leg_strikes = fighter_b_leg_strikes.strip()
                fighter_b_leg_strikes_landed, fighter_b_leg_strikes_attempted = fighter_b_leg_strikes.split('of')
                fighter_b_leg_strikes_landed_dict[f'fighter_b_rd_{i}_leg_strikes_landed'] = fighter_b_leg_strikes_landed.strip()
                fighter_b_leg_strikes_attempted_dict[f'fighter_b_rd_{i}_leg_strikes_attempted'] = fighter_b_leg_strikes_attempted.strip()
                

        # Distance Strikes - table 2 column 7
        fighter_a_distance_strikes_landed_dict = {}
        fighter_a_distance_strikes_attempted_dict = {}
        fighter_b_distance_strikes_landed_dict = {}
        fighter_b_distance_strikes_attempted_dict = {}

        for i in range(1,6):
            fighter_a_distance_strikes = response.css(f'section.b-fight-details__section:nth-of-type(5) table.b-fight-details__table:nth-of-type(1) tbody:nth-of-type(1) tr.b-fight-details__table-row:nth-of-type({i}) td.b-fight-details__table-col:nth-of-type(7) p.b-fight-details__table-text:nth-child(1)::text').get()

            if fighter_a_distance_strikes is not None:
                fighter_a_distance_strikes = fighter_a_distance_strikes.strip()
                fighter_a_distance_strikes_landed, fighter_a_distance_strikes_attempted = fighter_a_distance_strikes.split('of')
                fighter_a_distance_strikes_landed_dict[f'fighter_a_rd_{i}_distance_strikes_landed'] = fighter_a_distance_strikes_landed.strip()
                fighter_a_distance_strikes_attempted_dict[f'fighter_a_rd_{i}_distance_strikes_attempted'] = fighter_a_distance_strikes_attempted.strip()
        
        #Fighter B Distance Strikes
        for i in range(1,6):
            fighter_b_distance_strikes = response.css(f'section.b-fight-details__section:nth-of-type(5) table.b-fight-details__table:nth-of-type(1) tbody:nth-of-type(1) tr.b-fight-details__table-row:nth-of-type({i}) td.b-fight-details__table-col:nth-of-type(7) p.b-fight-details__table-text:nth-child(2)::text').get()

            if fighter_b_distance_strikes is not None:
                fighter_b_distance_strikes = fighter_b_distance_strikes.strip()
                fighter_b_distance_strikes_landed, fighter_b_distance_strikes_attempted = fighter_b_distance_strikes.split('of')
                fighter_b_distance_strikes_landed_dict[f'fighter_b_rd_{i}_distance_strikes_landed'] = fighter_b_distance_strikes_landed.strip()
                fighter_b_distance_strikes_attempted_dict[f'fighter_b_rd_{i}_distance_strikes_attempted'] = fighter_b_distance_strikes_attempted.strip()
                
        
        # Clinch Strikes - table 2 column 8
        fighter_a_clinch_strikes_landed_dict = {}
        fighter_a_clinch_strikes_attempted_dict = {}
        fighter_b_clinch_strikes_landed_dict = {}
        fighter_b_clinch_strikes_attempted_dict = {}

        for i in range(1,6):
            fighter_a_clinch_strikes = response.css(f'section.b-fight-details__section:nth-of-type(5) table.b-fight-details__table:nth-of-type(2) tbody:nth-of-type(1) tr.b-fight-details__table-row:nth-of-type({i}) td.b-fight-details__table-col:nth-of-type(7) p.b-fight-details__table-text:nth-child(1)::text').get()

            if fighter_a_clinch_strikes is not None:
                fighter_a_clinch_strikes = fighter_a_clinch_strikes.strip()
                fighter_a_clinch_strikes_landed, fighter_a_clinch_strikes_attempted = fighter_a_clinch_strikes.split('of')
                fighter_a_clinch_strikes_landed_dict[f'fighter_a_rd_{i}_clinch_strikes_landed'] = fighter_a_clinch_strikes_landed.strip()
                fighter_a_clinch_strikes_attempted_dict[f'fighter_a_rd_{i}_clinch_strikes_attempted'] = fighter_a_clinch_strikes_attempted.strip()

        #Fighter B Clinch Strikes
        for i in range(1,6):
            fighter_b_clinch_strikes = response.css(f'section.b-fight-details__section:nth-of-type(5) table.b-fight-details__table:nth-of-type(2) tbody:nth-of-type(1) tr.b-fight-details__table-row:nth-of-type({i}) td.b-fight-details__table-col:nth-of-type(7) p.b-fight-details__table-text:nth-child(2)::text').get()

            if fighter_b_clinch_strikes is not None:
                fighter_b_clinch_strikes = fighter_b_clinch_strikes.strip()
                fighter_b_clinch_strikes_landed, fighter_b_clinch_strikes_attempted = fighter_b_clinch_strikes.split('of')
                fighter_b_clinch_strikes_landed_dict[f'fighter_b_rd_{i}_clinch_strikes_landed'] = fighter_b_clinch_strikes_landed.strip()
                fighter_b_clinch_strikes_attempted_dict[f'fighter_b_rd_{i}_clinch_strikes_attempted'] = fighter_b_clinch_strikes_attempted.strip()
                
        
        # Ground Strikes - table 2 column 9
        
        fighter_a_ground_strikes_landed_dict = {}
        fighter_a_ground_strikes_attempted_dict = {}
        fighter_b_ground_strikes_landed_dict = {}
        fighter_b_ground_strikes_attempted_dict = {}

        for i in range(1,6):
            fighter_a_ground_strikes = response.css(f'section.b-fight-details__section:nth-of-type(5) table.b-fight-details__table:nth-of-type(3) tbody:nth-of-type(1) tr.b-fight-details__table-row:nth-of-type({i}) td.b-fight-details__table-col:nth-of-type(7) p.b-fight-details__table-text:nth-child(1)::text').get()

            if fighter_a_ground_strikes is not None:
                fighter_a_ground_strikes = fighter_a_ground_strikes.strip()
                fighter_a_ground_strikes_landed, fighter_a_ground_strikes_attempted = fighter_a_ground_strikes.split('of')
                fighter_a_ground_strikes_landed_dict[f'fighter_a_rd_{i}_ground_strikes_landed'] = fighter_a_ground_strikes_landed.strip()
                fighter_a_ground_strikes_attempted_dict[f'fighter_a_rd_{i}_ground_strikes_attempted'] = fighter_a_ground_strikes_attempted.strip()

        #Fighter B Ground Strikes
        for i in range(1,6):
            fighter_b_ground_strikes = response.css(f'section.b-fight-details__section:nth-of-type(5) table.b-fight-details__table:nth-of-type(3) tbody:nth-of-type(1) tr.b-fight-details__table-row:nth-of-type({i}) td.b-fight-details__table-col:nth-of-type(7) p.b-fight-details__table-text:nth-child(2)::text').get()

            if fighter_b_ground_strikes is not None:
                fighter_b_ground_strikes = fighter_b_ground_strikes.strip()
                fighter_b_ground_strikes_landed, fighter_b_ground_strikes_attempted = fighter_b_ground_strikes.split('of')
                fighter_b_ground_strikes_landed_dict[f'fighter_b_rd_{i}_ground_strikes_landed'] = fighter_b_ground_strikes_landed.strip()
                fighter_b_ground_strikes_attempted_dict[f'fighter_b_rd_{i}_ground_strikes_attempted'] = fighter_b_ground_strikes_attempted.strip()



        # Process the data

        # trim the whitespace
        event_link = event_link.strip() if event_link else None
        date = date.strip() if date else None
        location = location.strip() if location else None
        event_title = event_title.strip() if event_title else None
        fighter_a_name = fighter_a_name.strip() if fighter_a_name else None
        fighter_b_name = fighter_b_name.strip() if fighter_b_name else None
        #fighter_a_nickname = fighter_a_nickname.strip() if fighter_a_nickname else None
        #fighter_b_nickname = fighter_b_nickname.strip() if fighter_b_nickname else None
        fighter_a_result = fighter_a_result.strip() if fighter_a_result else None
        fighter_b_result = fighter_b_result.strip() if fighter_b_result else None

        #print("weight_class_with_tag:", weight_class_with_tag)
        #print("weight_class_no_tag:", weight_class_no_tag)


        if "weight" not in weight_class_with_tag:
            weight_class = weight_class_no_tag.strip()
        else:
            weight_class = weight_class_with_tag.strip()

        #print("weight_class:", weight_class)
        method = method.strip() if method else None
        round_text = ''.join(filter(str.isdigit, round_info))  # Extract and clean up the round number
        #time_text = time_text.strip() if time_text else None
        # Process to extract time_text
        if time_block:
            time_text = time_block.split('</i>')[1].strip() if '</i>' in time_block else None
        else:
            time_text = None
        

    #time_format_text = time_format_text.strip() if time_format_text else None
        # Process to extract time_format_text
        if time_format_block:
            time_format_text = time_format_block.split('</i>')[1].strip() if '</i>' in time_format_block else None
        else:
            time_format_text = None
        referee_text = referee_text.strip() if referee_text else None

        # Processing detail_description
        # Parse the HTML content with BeautifulSoup
        # if method does not include "Decision"
        if 'Decision' not in method:
            soup = BeautifulSoup(detail_description, 'html.parser')
            text_inside_p_tag = soup.find('p', class_='b-fight-details__text').text.strip()
            cleaned_text = re.sub(r'\s+', ' ', text_inside_p_tag).strip()
            cleaned_text = cleaned_text.replace('Details: ', '')
            cleaned_text = cleaned_text.replace('\n', '')
            detail_description = cleaned_text.split('... ', 1)[-1].strip()

            judge_a_name = None
            judge_b_name = None
            judge_c_name = None
            judge_a_score = None
            judge_b_score = None
            judge_c_score = None
        else:
            detail_description = None
            judge_a_name = judge_a_name.strip() if judge_a_name else None
            judge_b_name = judge_b_name.strip() if judge_b_name else None
            judge_c_name = judge_c_name.strip() if judge_c_name else None
            judge_a_score = judge_a_score.strip() if judge_a_score else None
            judge_b_score = judge_b_score.strip() if judge_b_score else None
            judge_c_score = judge_c_score.strip() if judge_c_score else None



        yield {
            'event_link': event_link,
            'date': date,
            'location': location,
            'event_title': event_title,
            'fighter_a_name': fighter_a_name,
            'fighter_b_name': fighter_b_name,
            #'fighter_a_nickname': fighter_a_nickname,
            #'fighter_b_nickname': fighter_b_nickname,
            'fighter_a_result': fighter_a_result,
            'fighter_b_result': fighter_b_result,
            'weight_class': weight_class,
            'method': method,
            'round_text': round_text,
            'time_text': time_text,
            'time_format_text': time_format_text,
            'referee_text': referee_text,
            'detail_description': detail_description,
            'judge_a_name': judge_a_name,
            'judge_b_name': judge_b_name,
            'judge_c_name': judge_c_name,
            'judge_a_score': judge_a_score,
            'judge_b_score': judge_b_score,
            'judge_c_score': judge_c_score,
            **fighter_a_kd_dict,
            **fighter_b_kd_dict,
            **fighter_a_sig_strikes_landed_dict,
            **fighter_a_sig_strikes_attempted_dict,
            **fighter_b_sig_strikes_landed_dict,
            **fighter_b_sig_strikes_attempted_dict,
            **fighter_a_total_strikes_landed_dict,
            **fighter_a_total_strikes_attempted_dict,
            **fighter_b_total_strikes_landed_dict,
            **fighter_b_total_strikes_attempted_dict,
            **fighter_a_takedowns_landed_dict,
            **fighter_a_takedowns_attempted_dict,
            **fighter_b_takedowns_landed_dict,
            **fighter_b_takedowns_attempted_dict,
            **fighter_a_sub_attempts_dict,
            **fighter_b_sub_attempts_dict,
            **fighter_a_reversals_dict,
            **fighter_b_reversals_dict,
            **fighter_a_control_dict,
            **fighter_b_control_dict,
            **fighter_a_head_strikes_landed_dict,
            **fighter_a_head_strikes_attempted_dict,
            **fighter_b_head_strikes_landed_dict,
            **fighter_b_head_strikes_attempted_dict,
            **fighter_a_body_strikes_landed_dict,
            **fighter_a_body_strikes_attempted_dict,
            **fighter_b_body_strikes_landed_dict,
            **fighter_b_body_strikes_attempted_dict,
            **fighter_a_leg_strikes_landed_dict,
            **fighter_a_leg_strikes_attempted_dict,
            **fighter_b_leg_strikes_landed_dict,
            **fighter_b_leg_strikes_attempted_dict,
            **fighter_a_distance_strikes_landed_dict,
            **fighter_a_distance_strikes_attempted_dict,
            **fighter_b_distance_strikes_landed_dict,
            **fighter_b_distance_strikes_attempted_dict,
            **fighter_a_clinch_strikes_landed_dict,
            **fighter_a_clinch_strikes_attempted_dict,
            **fighter_b_clinch_strikes_landed_dict,
            **fighter_b_clinch_strikes_attempted_dict,
            **fighter_a_ground_strikes_landed_dict,
            **fighter_a_ground_strikes_attempted_dict,
            **fighter_b_ground_strikes_landed_dict,
            **fighter_b_ground_strikes_attempted_dict
        }