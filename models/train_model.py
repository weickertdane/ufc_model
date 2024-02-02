import sqlite3
import pandas as pd
import json
import os
import logging
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report
import joblib

log_file_path = 'models/logs/train_model.log'
logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')



def get_db_connection(db_path):
    return sqlite3.connect(db_path)

def load_and_preprocess_data(cursor, parent_dir):

    # select all data from the calc_ufc_table
    cursor.execute('SELECT * FROM training_data')
    data = cursor.fetchall()

    # convert to a pandas dataframe
    df = pd.DataFrame(data, columns=['weight_class', 'cage_size', 'fighter_a_age_diff', 'fighter_a_eff_diff', 'fighter_a_control_rate_diff', 'fighter_a_result'])
    logging.info(f"DataFrame shape: {df.shape}")
    logging.info(f"DataFrame sample: {df.head()}")


    # One hot encode 'weight_class' and 'cage_size'
    encoder = OneHotEncoder()
    encoded_features = encoder.fit_transform(df[['weight_class', 'cage_size']])

    # Save the encoder
    encoder_path = os.path.join(parent_dir, 'models/encoder.joblib')
    joblib.dump(encoder, encoder_path)

    # Convert the sparse matrix to a dense format and create a DataFrame
    encoded_df = pd.DataFrame(encoded_features.toarray(), columns=encoder.get_feature_names_out(['weight_class', 'cage_size']))

    logging.info(f"Encoded DataFrame shape: {encoded_df.shape}")
    logging.info(f"Encoded DataFrame sample: {encoded_df.head()}")

    # Drop original 'weight_class' and 'cage_size' columns
    df.drop(['weight_class', 'cage_size'], axis=1, inplace=True)

    # Concatenate original dataframe with the encoded dataframe
    df = pd.concat([df, encoded_df], axis=1)

    # Log count of NaNs before dropping
    nan_count_before = df['fighter_a_result'].isnull().sum()
    logging.info(f"NaN count in 'fighter_a_result' before dropping: {nan_count_before}")

    # Drop NaNs
    df.dropna(subset=['fighter_a_result'], inplace=True)

    # Reset index after dropping rows
    df.reset_index(drop=True, inplace=True)

    # Log count of NaNs after dropping
    nan_count_after = df['fighter_a_result'].isnull().sum()
    logging.info(f"NaN count in 'fighter_a_result' after dropping: {nan_count_after}")

    # Convert 'result' to binary (1 for 'W', 0 for 'L')
    df['fighter_a_result'] = df['fighter_a_result'].map({'W': 1, 'L': 0})

    return df

def train_model(df):
    
    # Split data into X and y
    X = df.drop('fighter_a_result', axis=1)
    y = df['fighter_a_result']

    # Split into training and test sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

    # log X_train as a list so I can see the entire list in the log file
    logging.info(f"X_train: {X_train.columns.tolist()}")

    # Train the model
    model = RandomForestClassifier(random_state=42)
    model.fit(X_train, y_train)

    # Make predictions
    y_pred = model.predict(X_test)

    # Evaluate the model
    accuracy = accuracy_score(y_test, y_pred)
    report = classification_report(y_test, y_pred)

    return model, accuracy, report

def save_model(model, model_path):
    logging.info(f"Model saved to {model_path}")
    joblib.dump(model, model_path)

def load_model(model_path):
    logging.info(f"Model loaded from {model_path}")
    return joblib.load(model_path)

def main():

    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(current_dir)
    db_path = os.path.join(parent_dir, 'database/historical_raw.db') 
    model_save_path = os.path.join(parent_dir, 'models/rf_win_prob_model.joblib')
    try:
        conn = get_db_connection(db_path)
        cursor = conn.cursor()
        logging.info("Connected to database")

        # Call functions
        df = load_and_preprocess_data(cursor)
        model, accuracy, report = train_model(df)

        # Save model
        save_model(model, model_save_path)

        # Print results
        print(f"Accuracy: {accuracy}")
        logging.info(f"Accuracy: {accuracy}")
        print(f"Classification Report:\n{report}")
        logging.info(f"Classification Report:\n{report}")
        #log X_train to list
    
    except sqlite3.Error as e:
        logging.error(f"SQLite error: {e}")


if __name__ == "__main__":
    main()
