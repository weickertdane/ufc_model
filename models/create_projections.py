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

log_file_path = 'models/logs/create_projections.log'
logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')



def get_db_connection(db_path):
    return sqlite3.connect(db_path)


def get_upcoming_bouts(conn):
    query = """
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
    """
    return pd.read_sql_query(query, conn)


def insert_weight_class_averages(model_data):

    # Fill missing numeric values with the average of the weight class
    avg_values = model_data.groupby('weight_class').transform('mean')
    cols_to_replace = ['fighter_a_age_diff', 'fighter_a_eff_diff', 'fighter_a_control_rate_diff']
    model_data[cols_to_replace] = model_data[cols_to_replace].fillna(avg_values[cols_to_replace])
    return model_data

def gather_model_inputs(model_data, encoder):
    model_data = insert_weight_class_averages(model_data)

    # Apply One Hot Encoding to the categorical variables
    encoded_categories = encoder.transform(model_data[['weight_class', 'cage_size']]).toarray()
    encoded_df = pd.DataFrame(encoded_categories, columns=encoder.get_feature_names_out(['weight_class', 'cage_size']))

    # Concatenate the DataFrame with encoded categorical and numerical features
    model_data = pd.concat([model_data.drop(['weight_class', 'cage_size'], axis=1), encoded_df], axis=1)

    # Ensure we select only the features the model was trained on for prediction
    features_for_prediction = model_data[['fighter_a_age_diff', 'fighter_a_eff_diff', 'fighter_a_control_rate_diff', 
                                          'weight_class_Bantamweight', 'weight_class_Featherweight', 'weight_class_Flyweight', 
                                          'weight_class_Heavyweight', 'weight_class_Light Heavyweight', 'weight_class_Lightweight', 
                                          'weight_class_Middleweight', 'weight_class_Welterweight', "weight_class_Women's Bantamweight", 
                                          "weight_class_Women's Featherweight", "weight_class_Women's Flyweight", 
                                          "weight_class_Women's Strawweight", 'cage_size_big', 'cage_size_small']]
    return features_for_prediction

def make_projections(model_data, model, features_for_prediction):
    # Predict probabilities
    predictions = model.predict_proba(features_for_prediction)

    model_data['fighter_a_win_probability'] = predictions[:, 1].round(1)
    model_data['fighter_b_win_probability'] = 1 - model_data['fighter_a_win_probability']

    # Now round both probabilities after all calculations are done
    model_data['fighter_a_win_probability'] = model_data['fighter_a_win_probability'].round(1)
    model_data['fighter_b_win_probability'] = model_data['fighter_b_win_probability'].round(1)

    #round 'fighter_a_age_diff'
    model_data['fighter_a_age_diff'] = model_data['fighter_a_age_diff'].round(1)

    #order model data by date descending
    model_data = model_data.sort_values(by='date', ascending=True)

    return model_data


def main():

    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(current_dir)
    db_path = os.path.join(parent_dir, 'database/historical_raw.db')
    conn = None
    model_path = os.path.join(parent_dir, 'models/rf_win_prob_model.joblib')
    try:
        conn = get_db_connection(db_path)
        model_data = get_upcoming_bouts(conn)

        # Load the Saved Encoder
        encoder_path = os.path.join(parent_dir, 'models/encoder.joblib')
        encoder = joblib.load(encoder_path)

        # Load the model
        model = joblib.load(model_path)

        # Gather model inputs
        features_for_prediction = gather_model_inputs(model_data, encoder)

        # Make projections
        model_data = make_projections(model_data, model, features_for_prediction)
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