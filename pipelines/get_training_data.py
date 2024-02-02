import sqlite3
import logging
from datetime import datetime
import os

log_file_path = 'pipelines/logs/get_training_data.log'
logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_db_connection(db_path):
    return sqlite3.connect(db_path)

def get_data(cursor):
    cursor.execute("""
        SELECT 
            weight_class,
            cage_size,
            fighter_a_age_diff,
            fighter_a_eff_diff,
            fighter_a_control_rate_diff,
            fighter_a_result
        FROM calc_ufc_data
    """)
    return cursor.fetchall()

def insert_into_training_data(cursor, data):
    # Delete existing data
    cursor.execute("DELETE FROM training_data")

    # Insert new data
    cursor.executemany("""
        INSERT INTO training_data 
        (weight_class, cage_size, fighter_a_age_diff, fighter_a_eff_diff, fighter_a_control_rate_diff, fighter_a_result)
        VALUES (?, ?, ?, ?, ?, ?)
    """, data)

def clean_training_data(cursor):

    

    # delete rows where fighter_a_age_diff is null
    cursor.execute("""
        DELETE FROM training_data
        WHERE fighter_a_age_diff IS NULL
    """)
    # delete rows where weight_class contains 'Open' or 'Catch' or is blank
    cursor.execute("""
        DELETE FROM training_data
        WHERE weight_class LIKE '%Open%' OR weight_class LIKE '%Catch%' OR weight_class = ''
    """)
    # delete rows where cage_size is null
    cursor.execute("""
        DELETE FROM training_data
        WHERE cage_size IS NULL
    """)
    # delete rows where fighter_a_eff_diff = 0
    cursor.execute("""
        DELETE FROM training_data
        WHERE fighter_a_eff_diff = 0
    """)
    # delete rows where fighter_a_control_rate_diff = 0
    cursor.execute("""
        DELETE FROM training_data
        WHERE fighter_a_control_rate_diff = 0
    """)
    # delete rows where fighter_a_result = 'NC'
    cursor.execute("""
        DELETE FROM training_data
        WHERE fighter_a_result = 'NC'
    """)
    # delete rows where fighter_a_result = 'D
    cursor.execute("""
        DELETE FROM training_data
        WHERE fighter_a_result = 'D'
    """)

def main():

    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(current_dir)
    db_path = os.path.join(parent_dir, 'database/historical_raw.db')
    try:
        conn = get_db_connection(db_path)
        cursor = conn.cursor()
        logging.info("Connected to database")

        # Ensure the training_data table exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS training_data (
                weight_class TEXT,
                cage_size TEXT,
                fighter_a_age_diff REAL,
                fighter_a_eff_diff REAL,
                fighter_a_control_rate_diff REAL,
                fighter_a_result TEXT
            )
        """)

        # Retrieve data
        data = get_data(cursor)
        logging.info(f"Retrieved {len(data)} rows of data")

        # Insert data into training_data
        insert_into_training_data(cursor, data)
        logging.info(f"Inserted {len(data)} rows of data into training_data")

        # Clean the training_data table
        clean_training_data(cursor)
        logging.info("Cleaned training_data table")

        conn.commit()
        logging.info("Committed changes to database")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
