import sqlite3
import logging
from datetime import datetime
import os

def setup_logging():
    log_file_path = 'pipelines/logs/update_calcs_on_db.log'
    logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_db_connection(db_path):
    return sqlite3.connect(db_path)

def clean_weight_class(cursor):
    try:
        # Check if the 'calc_ufc_data' table exists and drop it if it does
        cursor.execute("DROP TABLE IF EXISTS calc_ufc_data")

        # Duplicate raw_ufc_data table as calc_ufc_data
        cursor.execute("CREATE TABLE calc_ufc_data AS SELECT * FROM raw_ufc_data")

        # List of substrings to remove
        substrings_to_remove = [' Bout', ' Title', 'UFC ', ' Interim', 'Interim ']

        # Loop through substrings and replace them in the weight_class column
        for substring in substrings_to_remove:
            cursor.execute(f"UPDATE calc_ufc_data SET weight_class = REPLACE(weight_class, '{substring}', '')")

    except sqlite3.Error as e:
        print(f"An error occurred: {e}")


def determine_cage_size(cursor):
        
    # Add a column called 'cage_size' to the calc_ufc_data table
    try:
        cursor.execute("ALTER TABLE calc_ufc_data ADD COLUMN cage_size TEXT")
    except sqlite3.Error as e:
        print("Error adding column 'cage_size' to calc_ufc_data:", e)
        return

    # Update the cage_size column for each row
    try:
        cursor.execute("UPDATE calc_ufc_data SET cage_size = 'small' WHERE date BETWEEN '05-30-2020' AND '06-27-2020'")
        cursor.execute("UPDATE calc_ufc_data SET cage_size = 'small' WHERE date > '03-20-2020' AND event_title LIKE '%Fight Night%' AND location LIKE '%Las Vegas%'")
        cursor.execute("UPDATE calc_ufc_data SET cage_size = 'big' WHERE cage_size IS NULL")
    except sqlite3.Error as e:
        print("Error updating 'cage_size' in calc_ufc_data:", e)
        return


def fighter_age_at_bouts(cursor):

    # Ensure the 'fighter_a_bout_age' and 'fighter_b_bout_age' columns exist in 'calc_ufc_data'
    try:
        cursor.execute("ALTER TABLE calc_ufc_data ADD COLUMN fighter_a_bout_age REAL")
        cursor.execute("ALTER TABLE calc_ufc_data ADD COLUMN fighter_b_bout_age REAL")
    except sqlite3.Error:
        # Columns already exist, continue
        pass

    # Retrieve 'date', 'fighter_a_name', and 'fighter_b_name' from 'calc_ufc_data'
    cursor.execute("SELECT date, fighter_a_name, fighter_b_name FROM calc_ufc_data")
    fights = cursor.fetchall()

    for fight_date, fighter_a, fighter_b in fights:
        # Calculate ages for fighter_a
        cursor.execute("SELECT dob FROM fighter_profile WHERE fighter = ?", (fighter_a,))
        dob_result_a = cursor.fetchone()

        if dob_result_a:
            dob_a = datetime.strptime(dob_result_a[0], "%m-%d-%Y").date()
            bout_date = datetime.strptime(fight_date, "%m-%d-%Y").date()
            age_at_bout_a = round((bout_date - dob_a).days / 365.25, 2)
        else:
            age_at_bout_a = None

        # Calculate ages for fighter_b
        cursor.execute("SELECT dob FROM fighter_profile WHERE fighter = ?", (fighter_b,))
        dob_result_b = cursor.fetchone()

        if dob_result_b:
            dob_b = datetime.strptime(dob_result_b[0], "%m-%d-%Y").date()
            # bout_date already calculated for fighter_a
            age_at_bout_b = round((bout_date - dob_b).days / 365.25, 2)
        else:
            age_at_bout_b = None

        # Update 'fighter_a_bout_age' and 'fighter_b_bout_age' in 'calc_ufc_data'
        cursor.execute("UPDATE calc_ufc_data SET fighter_a_bout_age = ?, fighter_b_bout_age = ? WHERE date = ? AND fighter_a_name = ? AND fighter_b_name = ?", (age_at_bout_a, age_at_bout_b, fight_date, fighter_a, fighter_b))


def age_difference(cursor):



    # Try to add the column, catch the exception if the column already exists
    try:
        cursor.execute("ALTER TABLE calc_ufc_data ADD COLUMN fighter_a_age_diff REAL")
    except sqlite3.OperationalError:
        pass  # Column already exists, so ignore the error

    try:
        cursor.execute("ALTER TABLE calc_ufc_data ADD COLUMN fighter_b_age_diff REAL")
    except sqlite3.OperationalError:
        pass  # Column already exists, so ignore the error

    # Update and round the values in the columns
    cursor.execute("UPDATE calc_ufc_data SET fighter_a_age_diff = ROUND(fighter_a_bout_age - fighter_b_bout_age, 2)")
    cursor.execute("UPDATE calc_ufc_data SET fighter_b_age_diff = ROUND(fighter_b_bout_age - fighter_a_bout_age, 2)")


import sqlite3


def duration_seconds(cursor):

    try:
        cursor.execute("ALTER TABLE calc_ufc_data ADD COLUMN duration_seconds INTEGER")
    except sqlite3.Error:
        # Columns already exist, continue
        pass
    try:
        # Retrieve 'date', 'fighter_a_name', and 'fighter_b_name' from 'calc_ufc_data'
        cursor.execute("SELECT round_text, time_text FROM calc_ufc_data")
        fights = cursor.fetchall()

        for round_text, time_text in fights:
            # Calculate the duration in seconds

            # split time_text into minutes and seconds
            minutes, seconds = map(int, time_text.split(":"))

            # Default case where round_text is 5 or 3 and time_text is 5:00
            if (round_text in [5, 3]) and time_text == "5:00":
                duration_seconds = round_text * 300
            else:
                duration_seconds = ((round_text * 300)-300) + (minutes * 60) + seconds
        
            # Update the 'duration_seconds' column
            cursor.execute("UPDATE calc_ufc_data SET duration_seconds=? WHERE round_text=? AND time_text=?", (duration_seconds, round_text, time_text))

            #conn.commit()

    except sqlite3.Error as e:
        print(f"An overall database error occurred: {e}")
        

def distance_bool(cursor):

    # Ensure the 'distance_bool' column exists in 'calc_ufc_data'
    try:
        cursor.execute("ALTER TABLE calc_ufc_data ADD COLUMN distance_bool INTEGER")
    except sqlite3.Error:
        pass  # Column already exists, continue

    # Retrieve 'round_text' and 'time_text' from 'calc_ufc_data'
    cursor.execute("SELECT round_text, time_text FROM calc_ufc_data")
    fights = cursor.fetchall()

    for round_text, time_text in fights:
        # Calculate the 'distance_bool' value
        distance_value = 1 if (round_text in [5, 3]) and time_text == "5:00" else 0
        
        # Update the 'distance_bool' column
        cursor.execute("UPDATE calc_ufc_data SET distance_bool=? WHERE round_text=? AND time_text=?", (distance_value, round_text, time_text))


# Function to get value safely
def get_value_from_db(row, key):

    return row[key] if key in row and row[key] is not None else 0

def add_column_if_not_exists(cursor, table_name, column_name, column_type):

    cursor.execute(f"SELECT * FROM {table_name} LIMIT 1")
    column_names = [description[0] for description in cursor.description]
    if column_name not in column_names:
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")


# Function to update database
def update_database(cursor, date, fighter_a, fighter_b, a_eff_diff, b_eff_diff, a_ctrl_diff, b_ctrl_diff):

    try:
        # Convert date object to string
        date_str = date.strftime('%m-%d-%Y')  # Ensure this format matches your database format

        cursor.execute('''
            UPDATE calc_ufc_data
            SET fighter_a_eff_diff = ?, fighter_b_eff_diff = ?, 
                fighter_a_control_rate_diff = ?, fighter_b_control_rate_diff = ?
            WHERE date = ? AND fighter_a_name = ? AND fighter_b_name = ?''', 
            (a_eff_diff, b_eff_diff, a_ctrl_diff, b_ctrl_diff, date_str, fighter_a, fighter_b))
    except Exception as e:
        logging.error(f"Error in update_database: {e}")
        raise
    
# Function to safely convert to float
def safe_float_convert(value, default=0.0):
    try:
        return float(value)
    except ValueError:
        return default

def calculate_efficiency_and_control_rate(cursor):

    try:
        # Retrieve fights data from the database
        cursor.execute('SELECT * FROM calc_ufc_data')
        rows = cursor.fetchall()

        # Function to convert date string to datetime object
        def convert_date(date_str):
            return datetime.strptime(date_str, '%m-%d-%Y')

        # Convert rows to a list of dictionaries and sort by date
        fights = []
        for row in rows:
            fight = {description[0]: row[i] for i, description in enumerate(cursor.description)}
            fight['date'] = convert_date(fight['date'])  # Convert date to datetime object
            fights.append(fight)

        # Sort the fights by the date column from earliest to latest
        fights.sort(key=lambda x: x['date'])

        # turn control time into seconds
        for fight in fights:
            for key, value in fight.items():
                if value is not None and 'control' in key and isinstance(value, str) and ':' in value:
                    minutes, seconds = map(int, value.split(':'))
                    total_control_seconds = minutes * 60 + seconds
                    fight[key] = total_control_seconds

        # Running totals
        head_strikes_data = {}

        # Iterate over each fight
        for fight in fights:
            fighter_a = fight['fighter_a_name']
            fighter_b = fight['fighter_b_name']
            date = fight['date']
            logging.info(f'Processing fight between {fighter_a} and {fighter_b} on {date}')

            # Initialize data for new fighters
            if fighter_a not in head_strikes_data:
                head_strikes_data[fighter_a] = {'net_head_strikes': 0, 'total_duration': 0}
                logging.info(f'{fighter_a} was not in head_strikes_data, so we added them')
            if fighter_b not in head_strikes_data:
                head_strikes_data[fighter_b] = {'net_head_strikes': 0, 'total_duration': 0}
                logging.info(f'{fighter_b} was not in head_strikes_data, so we added them')

            # Calculate efficiency for fighter_a
            net_head_strikes_a = head_strikes_data[fighter_a]['net_head_strikes']
            logging.info(f'{fighter_a} has {net_head_strikes_a} net head strikes')
            total_duration_a = head_strikes_data[fighter_a]['total_duration']
            logging.info(f'{fighter_a} has {total_duration_a} total duration')

            if total_duration_a > 0:
                efficiency_a = net_head_strikes_a / (total_duration_a / 60)
            else:
                efficiency_a = 0

            fight['fighter_a_head_strikes_efficiency_per_min'] = efficiency_a

            # Calculate efficiency for fighter_b
            net_head_strikes_b = head_strikes_data[fighter_b]['net_head_strikes']
            logging.info(f'{fighter_b} has {net_head_strikes_b} net head strikes')
            total_duration_b = head_strikes_data[fighter_b]['total_duration']
            logging.info(f'{fighter_b} has {total_duration_b} total duration')

            if total_duration_b > 0:
                efficiency_b = net_head_strikes_b / (total_duration_b / 60)
            else:
                efficiency_b = 0

            fight['fighter_b_head_strikes_efficiency_per_min'] = efficiency_b

            # Use safe_float_convert for all conversions
            efficiency_a = safe_float_convert(efficiency_a)
            logging.info(f'{fighter_a} has {efficiency_a} head strikes efficiency')
            efficiency_b = safe_float_convert(efficiency_b)
            logging.info(f'{fighter_b} has {efficiency_b} head strikes efficiency')

            # Calculate fighter_a_eff_diff and fighter_b_eff_diff
            fighter_a_eff_diff = efficiency_a - efficiency_b
            logging.info(f'{fighter_a} has {fighter_a_eff_diff} head strikes efficiency differential')
            fighter_b_eff_diff = efficiency_b - efficiency_a
            logging.info(f'{fighter_b} has {fighter_b_eff_diff} head strikes efficiency differential')

            fight['fighter_a_eff_diff'] = fighter_a_eff_diff
            fight['fighter_b_eff_diff'] = fighter_b_eff_diff

            # Update running totals with current fight data
            for round_no in range(1, 6):
                a_head_strikes_key = f'fighter_a_rd_{round_no}_head_strikes_landed'
                b_head_strikes_key = f'fighter_b_rd_{round_no}_head_strikes_landed'
                a_head_strikes = get_value_from_db(fight, a_head_strikes_key)
                b_head_strikes = get_value_from_db(fight, b_head_strikes_key)
                head_strikes_data[fighter_a]['net_head_strikes'] += (a_head_strikes - b_head_strikes)
                head_strikes_data[fighter_b]['net_head_strikes'] += (b_head_strikes - a_head_strikes)
            
            fight_duration = get_value_from_db(fight, 'duration_seconds')
            fight_duration = safe_float_convert(fight_duration)

            head_strikes_data[fighter_a]['total_duration'] += fight_duration
            head_strikes_data[fighter_b]['total_duration'] += fight_duration

        # CONROL TIME
            
        # Running totals
        control_data = {}

        # Iterate over each fight
        for fight in fights:
            fighter_a = fight['fighter_a_name']
            fighter_b = fight['fighter_b_name']

            # Initialize data for new fighters
            if fighter_a not in control_data:
                control_data[fighter_a] = {'control_time': 0, 'total_duration': 0}
            if fighter_b not in control_data:
                control_data[fighter_b] = {'control_time': 0, 'total_duration': 0}

            # Calculate running control rate for fighter_a
            control_time_a = control_data[fighter_a]['control_time']
            total_duration_a = control_data[fighter_a]['total_duration']

            if total_duration_a > 0:
                control_rate_a = control_time_a / total_duration_a
            else:
                control_rate_a = 0

            fight['fighter_a_control_rate'] = control_rate_a

            # Calculate running control rate for fighter_b
            control_time_b = control_data[fighter_b]['control_time']
            total_duration_b = control_data[fighter_b]['total_duration']

            if total_duration_b > 0:
                control_rate_b = control_time_b / total_duration_b
            else:
                control_rate_b = 0

            fight['fighter_b_control_rate'] = control_rate_b

            # Ensure the values are floats
            control_rate_a = safe_float_convert(control_rate_a)
            logging.info(f'{fighter_a} has {control_rate_a} control rate')
            control_rate_b = safe_float_convert(control_rate_b)
            logging.info(f'{fighter_b} has {control_rate_b} control rate')

            # Calculate control rate differentials headed into the bout
            fighter_a_control_rate_diff = control_rate_a - control_rate_b
            logging.info(f'{fighter_a} has {fighter_a_control_rate_diff} control rate differential')
            fighter_b_control_rate_diff = control_rate_b - control_rate_a
            logging.info(f'{fighter_b} has {fighter_b_control_rate_diff} control rate differential')

            fight['fighter_a_control_rate_diff'] = fighter_a_control_rate_diff
            fight['fighter_b_control_rate_diff'] = fighter_b_control_rate_diff

            # Update running totals with current fight data
            for round_no in range(1, 6):
                a_control_rate_key = f'fighter_a_rd_{round_no}_control'
                b_control_rate_key = f'fighter_b_rd_{round_no}_control'
                a_control_time = get_value_from_db(fight, a_control_rate_key)
                b_control_time = get_value_from_db(fight, b_control_rate_key)
                a_control_time = safe_float_convert(a_control_time)
                b_control_time = safe_float_convert(b_control_time)
                control_data[fighter_a]['control_time'] += (a_control_time - b_control_time)
                control_data[fighter_b]['control_time'] += (b_control_time- a_control_time)
            
            fight_duration = get_value_from_db(fight, 'duration_seconds')
            fight_duration = safe_float_convert(fight_duration)

            control_data[fighter_a]['total_duration'] += fight_duration
            control_data[fighter_b]['total_duration'] += fight_duration

        # Iterate over each fight and update the database
        for fight in fights:
            date = fight['date']
            fighter_a = fight['fighter_a_name']
            fighter_b = fight['fighter_b_name']
            a_eff_diff = fight['fighter_a_eff_diff']
            b_eff_diff = fight['fighter_b_eff_diff']
            a_ctrl_diff = fight['fighter_a_control_rate_diff']
            b_ctrl_diff = fight['fighter_b_control_rate_diff']

            # Ensure all variables are assigned
            a_eff_diff = round(safe_float_convert(a_eff_diff), 2)
            logging.info(f'{fighter_a} has {a_eff_diff} head strikes efficiency differential')
            b_eff_diff = round(safe_float_convert(b_eff_diff), 2)
            logging.info(f'{fighter_b} has {b_eff_diff} head strikes efficiency differential')
            a_ctrl_diff = round(safe_float_convert(a_ctrl_diff), 2)
            logging.info(f'{fighter_a} has {a_ctrl_diff} control rate differential')
            b_ctrl_diff =round(safe_float_convert(b_ctrl_diff), 2)
            logging.info(f'{fighter_b} has {b_ctrl_diff} control rate differential')

            # Update the database
            update_database(cursor, date, fighter_a, fighter_b, a_eff_diff, b_eff_diff, a_ctrl_diff, b_ctrl_diff)

        
    except Exception as e:
        logging.error(f"Error occurred in calculate_efficiency_and _control_rate: {e}")
        raise

def main():
    setup_logging()

    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(current_dir)
    db_path = os.path.join(parent_dir, 'database/historical_raw.db')    

    try:
        conn = get_db_connection(db_path)
        cursor = conn.cursor()

        # Call your functions here
        clean_weight_class(cursor)
        determine_cage_size(cursor)
        fighter_age_at_bouts(cursor)
        age_difference(cursor)
        duration_seconds(cursor)
        distance_bool(cursor)
        
        # New function calls
        add_column_if_not_exists(cursor, 'calc_ufc_data', 'fighter_a_eff_diff', 'REAL')
        add_column_if_not_exists(cursor, 'calc_ufc_data', 'fighter_b_eff_diff', 'REAL')
        add_column_if_not_exists(cursor, 'calc_ufc_data', 'fighter_a_control_rate_diff', 'REAL')
        add_column_if_not_exists(cursor, 'calc_ufc_data', 'fighter_b_control_rate_diff', 'REAL')

        calculate_efficiency_and_control_rate(cursor)

        conn.commit()
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
