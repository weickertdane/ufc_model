import subprocess
import logging

logging.basicConfig(filename='run_scripts.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Run scripts in the desired order
scripts_to_run = [
    "pipelines/add_recent_bouts_to_raw_db.py",
    "pipelines/update_calcs_on_db.py",
    "pipelines/get_training_data.py",
    "models/train_model.py",
    "pipelines/add_upcoming_bouts_to_table.py",
    "pipelines/add_calcs_to_upcoming_bouts.py",
    "models/create_projections.py"
]

try:
    for script in scripts_to_run:
        logging.info(f"Running {script}")
        subprocess.call(["python3", script])
        logging.info(f"Finished running {script}")
except Exception as e:
    logging.exception(f"Error running script: {e}")
    raise e
