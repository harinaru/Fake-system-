import logging
import subprocess

# Ensure logging is configured correctly
logging.basicConfig(
    filename='pipeline.log', 
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def run_script(script_name):
    try:
        logging.info(f"Starting {script_name}")
        subprocess.run(["python", script_name], check=True)
        logging.info(f"Successfully completed {script_name}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error in {script_name}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error in {script_name}: {e}")

def main():
    logging.info("Starting Fake News Detection Pipeline...")
    
    scripts = [
        "1_data_preprocessing.py"
    ]
    
    for script in scripts:
        run_script(script)
    
    logging.info("All modules executed successfully!")

if __name__ == "__main__":
    main()
