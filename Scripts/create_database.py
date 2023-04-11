import sqlalchemy as db
from sqlalchemy import create_engine,text, Table, Column, Integer, String, Numeric, MetaData, Date, ForeignKey, Boolean
import pandas as pd
from sqlalchemy_utils import database_exists, create_database
from dotenv import load_dotenv
import os

load_dotenv()  # Load environment variables from .env file

db_password = os.getenv("DB_PASSWORD")

engine = create_engine(f"mssql+pyodbc://sa:{db_password}@localhost:1433/Talent?driver=ODBC+Driver+17+for+SQL+Server")

try:
    if database_exists(engine.url) == False:
        create_database(engine.url)
except:
    create_database(engine.url)

# drop all tables
metadata = db.MetaData()
metadata.reflect(bind=engine)
metadata.drop_all(bind=engine)


conn = engine.connect()

metadata = MetaData()

table_name = 'Candidate_Details'
academy_table = Table(table_name, metadata,
                    Column('id', String(50), primary_key=True),
                    Column('name', String),
                    Column('gender', String),
                    Column('dob', String),
                    Column('email', String),
                    Column('city', String),
                    Column('address', String),
                    Column('postcode', String),
                    Column('phone_number', String),
                    Column('uni', String),
                    Column('degree', String),
                    Column('invited_date', String),
                    Column('invited_by', String),                  
                    )

# table_name = 'Sparta_Day_Results'
# talent_sparta_results = Table(table_name, metadata,
#                             Column('sparta_day_person_id', Integer, primary_key=True),
#                             Column('talent_id', Integer, ForeignKey('Candidate_Details.id')),
#                             Column('name', String),
#                             Column('date', Date),
#                             Column('self_development', Boolean),
#                             Column('geo_flex', Boolean),
#                             Column('financial_support_self', Boolean),
#                             Column('result', Integer),
#                             Column('course_interest', String),
#                             Column('psychometric_result', Integer),
#                             Column('presentation_result', Integer)
#                             )

# table_name = 'Behaviours'
# behaviours_table = Table(table_name, metadata, 
#                         Column('id', Integer, primary_key=True), 
#                         Column('talent_id', Integer, ForeignKey('Candidate_Details.id')), 
#                         Column('week', Integer), 
#                         Column('analytic', Integer), 
#                         Column('independent', Integer), 
#                         Column('imaginative', Integer), 
#                         Column('studious', Integer), 
#                         Column('professional', Integer), 
#                         Column('determined', Integer)
#                         )

# table_name = 'Strengths'
# strengths_table = Table(table_name, metadata,
#                     Column('talent_id', Integer,  primary_key=True),
#                     Column('strength', String(20)) 
#                     )

# table_name = 'Weaknesses'
# weakness_table = Table(table_name, metadata,
#                     Column('talent_id', Integer,  primary_key=True),
#                     Column('weakness', String(20)) 
#                     )

# table_name = 'Tech_Self_Scores'
# tech_table = Table(table_name, metadata,
#                    Column('id', Integer, ForeignKey('Sparta_Day_Results.sparta_day_person_id')),
#                    Column('techname', String),
#                    Column('score', Integer)
#                    )

metadata.create_all(engine)


conn.close()