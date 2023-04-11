import sqlalchemy as db
from sqlalchemy import create_engine,text, Table, Column, Integer, String, Numeric, MetaData, Date, ForeignKey, Boolean
import pandas as pd
from sqlalchemy_utils import database_exists
import os
from dotenv import load_dotenv
import os

from applicant_details_transformation import df

load_dotenv()
#Load environment variables from .env file

#class for Loading. It wll have 

db_password = os.getenv("DB_PASSWORD")

engine = create_engine(f"mssql+pyodbc://sa:{db_password}@localhost:1433/Talent?driver=ODBC+Driver+17+for+SQL+Server")

conn = engine.connect()

df = df.drop(['id', 'iddate', 'invited_date'], axis=1)

df.rename(columns={'uniqueid': 'id', 'Invited on': 'invited_date'}, inplace=True)

df.to_sql('Candidate_Details', conn, if_exists='append', index=False)
