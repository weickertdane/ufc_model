import sqlite3
import logging
from datetime import datetime
from datetime import date
import pandas as pd
import joblib
from sklearn.preprocessing import OneHotEncoder
import smtplib
from email.message import EmailMessage
import io
import os



def setup_logging():
    log_file_path = 'models/logs/create_projections.log'
    logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_db_connection(db_path):
    return sqlite3.connect(db_path)

def get_upcoming_bouts(cursor):
    cursor.execute("""
        SELECT 
            date,
            fighter_a_name,
            fighter_b_name,
            weight_class,
            cage_size,
            fighter_a_age_diff,
            fighter_a_eff_diff,
            fighter_a_control_rate_diff
        FROM upcoming_bouts_for_model
    """)
    model_data = cursor.fetchall()
    columns = ['date', 'fighter_a_name', 'fighter_b_name', 'weight_class', 'cage_size', 'fighter_a_age_diff', 'fighter_a_eff_diff', 'fighter_a_control_rate_diff']
    return pd.DataFrame(model_data, columns=columns)

def handle_missing_values(model_data):
    avg_values = model_data.groupby('weight_class').mean(numeric_only=True)


    for index, bout in model_data.iterrows():
        weight_class = bout['weight_class']
        for col in ['fighter_a_age_diff', 'fighter_a_eff_diff', 'fighter_a_control_rate_diff']:
            if pd.isna(bout[col]) or bout[col] == 0:
                model_data.at[index, col] = avg_values.at[weight_class, col]
    return model_data

  

def make_projections(model_data, encoder, model):
    model_data = handle_missing_values(model_data)

    # Apply One Hot Encoding to the categorical variables
    encoded_categories = encoder.transform(model_data[['weight_class', 'cage_size']]).toarray()
    encoded_df = pd.DataFrame(encoded_categories, columns=encoder.get_feature_names_out(['weight_class', 'cage_size']))

    # Drop original 'weight_class' and 'cage_size' columns from model_data
    model_data.drop(['weight_class', 'cage_size'], axis=1, inplace=True)

    # Concatenate original model_data with the encoded DataFrame
    model_data = pd.concat([model_data, encoded_df], axis=1)

    logging.info(f"Model data head: {model_data.head()}")

    for index, bout in model_data.iterrows():
        # Select the relevant features for prediction
        features = bout.drop(['date', 'fighter_a_name', 'fighter_b_name']).values.reshape(1, -1)

        # Log the 15 feature headers before prediction
        logging.info(f"Feature headers: {list(model_data.drop(['date', 'fighter_a_name', 'fighter_b_name'], axis=1).columns)}")

        logging.info(f"Features for prediction: {list(features[0])}")

        fighter_a_win_probability = round(model.predict_proba(features)[:, 1][0], 3)
        model_data.at[index, 'fighter_a_win_probability'] = fighter_a_win_probability
        model_data.at[index, 'fighter_b_win_probability'] = 1 - fighter_a_win_probability

    return model_data



def main():
    setup_logging()

    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(current_dir)
    db_path = os.path.join(parent_dir, 'database/historical_raw.db')
    conn = None
    model_path = os.path.join(parent_dir, 'models/rf_win_prob_model.joblib')
    try:
        conn = get_db_connection(db_path)
        cursor = conn.cursor()
        model_data = get_upcoming_bouts(cursor)

        # Load the Saved Encoder
        encoder_path = os.path.join(parent_dir, 'models/encoder.joblib')
        encoder = joblib.load(encoder_path)

        # Load the model
        model = joblib.load(model_path)

        model_data = make_projections(model_data, encoder, model)
        logging.info(f"Model data head: {model_data.head()}")

        # Get today's date in YYYY-MM-DD format
        today_date = date.today().strftime("%Y-%m-%d")
        projections_path = os.path.join(parent_dir, f'models/projection_outputs/projections_{today_date}.csv')
        model_data.to_csv(projections_path, index=False)

        
    except Exception as e:
        logging.error("An error occurred", exc_info=True)  # Enhanced error logging
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()