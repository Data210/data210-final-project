from sqlalchemy import create_engine
from config_data_analyst_logic import create_connection_string

engine = create_engine(create_connection_string())
print(engine)