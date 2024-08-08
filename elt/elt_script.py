import subprocess
import time
import logging

logging.basicConfig(level=logging.INFO)

def wait_for_postgres(host, max_retries=5, delay_seconds=5):
    retries = 0
    while retries < max_retries:
        try:
            result = subprocess.run(
                ["pg_isready", "-h", host], check=True, capture_output=True, text=True
            )
            if "accepting connections" in result.stdout:
                logging.info("Successfully connected to PostgreSQL")
                return True
        except subprocess.CalledProcessError as e:
            logging.error(f"Error connecting to PostgreSQL: {e}")
            retries += 1
            logging.info(f"Retrying in {delay_seconds} seconds... (attempt {retries}/{max_retries})")
            time.sleep(delay_seconds)

    logging.error("Max retries reached. Exiting")
    return False

def run_pg_dump(source_config):
    dump_command = [
        'pg_dump',
        '-h', source_config['host'],
        '-U', source_config['user'],
        '-d', source_config['dbname'],
        '-f', 'data_dump.sql',
        '-w'
    ]
    subprocess_env = dict(PGPASSWORD=source_config['password'])
    subprocess.run(dump_command, env=subprocess_env, check=True)

def run_psql_load(destination_config):
    load_command = [
        'psql',
        '-h', destination_config['host'],
        '-U', destination_config['user'],
        '-d', destination_config['dbname'],
        '-a', '-f', 'data_dump.sql'
    ]
    subprocess_env = dict(PGPASSWORD=destination_config['password'])
    subprocess.run(load_command, env=subprocess_env, check=True)

if __name__ == "__main__":
    source_config = {
        'dbname': 'source_db',
        'user': 'postgres',
        'password': 'secret',
        'host': 'source_postgres'
    }

    destination_config = {
        'dbname': 'destination_db',
        'user': 'postgres',
        'password': 'secret',
        'host': 'destination_postgres'
    }

    if not wait_for_postgres(host=source_config['host']):
        exit(1)

    logging.info("Starting ELT Script...")

    run_pg_dump(source_config)
    run_psql_load(destination_config)

    logging.info("Ending ELT Script")

