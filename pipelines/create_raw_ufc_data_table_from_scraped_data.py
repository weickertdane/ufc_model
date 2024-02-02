# find the item with the most keys not has no null values
# and use that to create the table

import sqlite3
import json
from datetime import datetime

# File paths
json_file_path = "/Users/daneweickert/Library/CloudStorage/GoogleDrive-weickertdane99@gmail.com/My Drive/Work/Sports Betting/Sports/MMA/ufc_modeling/scraping_UFCstats/scraping_ufc_stats/scraping_ufc_stats/spiders/historical.json"
db_file_path = "/Users/daneweickert/Library/CloudStorage/GoogleDrive-weickertdane99@gmail.com/My Drive/Work/Sports Betting/Sports/MMA/ufc_modeling/prod/database/historical_raw.db"  # Change to your desired database file path

def convert_date(json_file_path):
    with open(json_file_path, 'r') as file:
        json_data = json.load(file)

    for item in json_data:
        date_str = item.get('date', '').strip()  # Trim whitespace
        print(f'date_str: {date_str}')
        if date_str:
            try:
                date_obj = datetime.strptime(date_str, '%B %d, %Y')  # Ensure format matches
                item['date'] = date_obj.strftime('%m-%d-%Y')
                #print data item after conversion
                print(f'Date in Item: {item["date"]}')


            except ValueError as e:
                print(f"Error parsing date '{date_str}': {e}")

    return json_data


def get_columns_and_data_types(json_data):

    # each item can have a different number of keys,
        # depending on how long the fight lasted
    # so we need to find the maximum number of keys so the table
        # has every possible key value

    # Initialize variables to keep track of the maximum number of keys 
        # and the corresponding item
    max_keys = 0
    item_with_max_keys = None

    # find the item with the most keys
    for item in (json_data):
        #print(item.values())
        # count the number of keys in each item
        num_keys = len(item.keys())
        #print(item)

        # if num_keys is greater than the current max, update the max
        if num_keys > max_keys:
            max_keys = num_keys
            item_with_max_keys = item
            #print(item_with_max_keys)

    # Add data types to the item_with_max_keys
    
    text_keys = [
        'event_link', 'date', 'location', 'event_title','fighter_a_name','fighter_b_name','fighter_a_result','fighter_b_result',
        'weight_class','method','time_text','time_format_text','referee_text','detail_description',
        'judge_a_name','judge_b_name','judge_c_name','judge_a_score','judge_b_score','judge_c_score',
        'control']
    
    int_keys = ['round_text']

    # Fighter designations
    fighters = ['fighter_a', 'fighter_b']

    # Round designations
    rounds = ['rd_1', 'rd_2', 'rd_3', 'rd_4', 'rd_5']

    # Specific statistics
    statistics = [
        'kd','sig_str_landed','sig_str_attempted','total_str_landed','total_str_attempted',
        'takedowns_landed','takedowns_attempted','sub_attempts','reversals','head_strikes_landed','head_strikes_attempted',
        'body_strikes_landed','body_strikes_attempted','leg_strikes_landed','leg_strikes_attempted',
        'distance_strikes_landed','distance_strikes_attempted','clinch_strikes_landed','clinch_strikes_attempted',
        'ground_strikes_landed','ground_strikes_attempted'
    ]

    # Generate the keys
    for fighter in fighters:
        for round_num in rounds:
            for stat in statistics:
                key = f"{fighter}_{round_num}_{stat}"
                int_keys.append(key)

    # Assign data types to the keys
    for key, value in item_with_max_keys.items():
        if key in text_keys:
            # Assign data type to string
            item_with_max_keys[key] = str(value) if value is not None else 'TEXT'
        elif key in int_keys:
            # Assign data type to int, use 0 as default if value is None
            item_with_max_keys[key] = int(value) if value is not None else 0

    return item_with_max_keys

# Function to create a table dynamically based on the JSON keys
def create_table(cursor, item_with_max_keys):
    

    # Define a mapping from Python types to SQLite types
    type_mapping = {
        int: "INTEGER",
        float: "REAL",
        str: "TEXT"
        # Add more mappings if needed
    }


    # Generate column definitions
    columns = []
    for key, value in item_with_max_keys.items():
        if value is None:
            # If the value is None, set the column type to TEXT
            columns.append(f"{key} TEXT")
        else:
            columns.append(f"{key} {type_mapping[type(value)]}")

    columns_str = ', '.join(columns)
    create_table_query = f"CREATE TABLE IF NOT EXISTS raw_ufc_data ({columns_str});"
    cursor.execute(create_table_query)

# Function to insert data into the table
def insert_data(cursor, json_data):
    for item in json_data:
        columns = ', '.join(item.keys())
        placeholders = ', '.join('?' * len(item))
        insert_query = f"INSERT INTO raw_ufc_data ({columns}) VALUES ({placeholders})"
        cursor.execute(insert_query, list(item.values()))

# Main script
# Create a connection to the SQLite database
conn = sqlite3.connect(db_file_path)
cursor = conn.cursor()

# Load and convert JSON data
json_data = convert_date(json_file_path)  # Use the returned modified data

# Create table and insert data
convert_date(json_file_path)
item_with_max_keys = get_columns_and_data_types(json_data)
create_table(cursor, item_with_max_keys)
insert_data(cursor, json_data)




# Commit changes and close the connection
conn.commit()
conn.close()