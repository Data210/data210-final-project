import configparser
from dotenv import load_dotenv
import os


def create_connection_string():
    # create a configparser object
    config = configparser.ConfigParser()

    # read the contents of the config file
    config.read('config_file.ini')

    # get the values of the databases section
    # database = config['database']
    # driver = database['driver']
    # server = database['server']
    # port = database['port']
    # database_name = database['database_name']
    # dialect = database['dialect']

    driver = os.getenv('DB_DRIVER')
    server = os.getenv('DB_SERVER')
    port = os.getenv('DB_PORT')
    database_name = os.getenv('DB_NAME')
    dialect = os.getenv('DB_DIALECT')

    load_dotenv()  # Load environment variables from .env file

    db_password = os.getenv("DB_PASSWORD")
    db_username = os.getenv("DB_USERNAME")
    connection_string = f"{dialect}://{db_username}:{db_password}@{server}:{port}/{database_name}?driver={driver}"
    return connection_string
