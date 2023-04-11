import sqlalchemy as db
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Date, ForeignKey, Boolean
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

# define tables
table_name = 'Recruiters'
recruiters_table = Table (table_name, metadata,
                          Column('recruiter_id', Integer, primary_key=True, autoincrement=True),
                          Column('recruiter_name', String)
                          )

table_name = 'Applicants'
applicants_table = Table(table_name, metadata,
                    Column('applicant_id', String(50), primary_key=True),
                    Column('name', String),
                    Column('gender', String),
                    Column('dob', String),
                    Column('email', String),
                    Column('phone_number', String),
                    Column('uni', String),
                    Column('degree', String),
                    Column('invited_date', String),
                    Column('recruiter_id', Integer, ForeignKey('Recruiters.recruiter_id'))                  
                    )

table_name = 'Location'
location_table = Table (table_name, metadata,
                        Column('location_id', Integer, primary_key=True, autoincrement=True),
                        Column('applicant_id', String(50), ForeignKey('Applicants.applicant_id')),
                        Column('address', String),
                        Column('postcode', String),
                        Column('city', String)
                        )

table_name = 'Academy_Locations'
Academy_location_table = Table (table_name, metadata,
                    Column('academy_location_id', Integer, primary_key=True, autoincrement=True),
                    Column('location_name', String)
                    )

table_name = 'Talent'
talent_table = Table(table_name, metadata,
                            Column('applicant_id', String(50), ForeignKey('Applicants.applicant_id'), primary_key=True, unique=True),
                            Column('date', Date, nullable=False),
                            Column('self_development', Boolean),
                            Column('financial_support_self', Boolean),
                            Column('result', Integer),
                            Column('course_interest', String),
                            Column('psychometric_result', Integer),
                            Column('presentation_result', Integer),
                            Column('geo_flex', Boolean),
                            Column('academy_location_id', Integer, ForeignKey('Academy_Locations.academy_location_id'))
                            )

table_name = 'Behaviours'
behaviours_table = Table(table_name, metadata, 
                        Column('behaviour_id', Integer, primary_key=True, autoincrement=True), 
                        Column('applicant_id', String(50), ForeignKey('Applicants.applicant_id'), unique=True), 
                        Column('week_number', Integer, nullable=False), 
                        Column('analytic', Integer), 
                        Column('independent', Integer), 
                        Column('imaginative', Integer), 
                        Column('studious', Integer), 
                        Column('professional', Integer), 
                        Column('determined', Integer)
                        )

table_name = 'Strengths'
strengths_table = Table(table_name, metadata,
                    Column('strength_id', Integer, primary_key=True),
                    Column('strength', String(30), unique=True, nullable=False)
                    )

table_name = 'Weaknesses'
strengths_table = Table(table_name, metadata,
                    Column('weakness_id', Integer, primary_key=True),
                    Column('weakness', String(30), unique=True, nullable=False)
                    )

table_name = 'Strengths_Junction'
strengths_junction = Table(table_name,metadata,
                        Column('applicant_id', String(50), ForeignKey('Applicants.applicant_id')),
                        Column('strength_id', Integer, ForeignKey('Strengths.strength_id'))
                        )

table_name = 'Weaknesses_Junction'
weaknesses_junction = Table(table_name,metadata,
                        Column('applicant_id', String(50), ForeignKey('Applicants.applicant_id')),
                        Column('strength_id', Integer)
                        )

table_name = 'Tech_Self_Scores'
tech_table = Table(table_name, metadata,
                   Column('tech_id', Integer, primary_key=True, autoincrement=True),
                   Column('tech_name', String(50), unique=True, nullable=False)
                   )

table_name = 'Tech_Junction'
tech_junction_table = Table(table_name, metadata,
                            Column('tech_id', Integer, ForeignKey("Tech_Self_Scores")),
                            Column('applicant_id', String(50), ForeignKey('Applicants.applicant_id')),
                            Column('score', Integer, nullable=False)
                            )

table_name = 'Trainers'
trainers_table = Table(table_name, metadata,
                        Column('trainer_id', Integer, primary_key=True, autoincrement=True),
                        Column('trainer', String(50), nullable=False)
                        )

table_name = 'Courses'
courses_table = Table(table_name, metadata,
                      Column('course_id', Integer, primary_key=True, autoincrement=True),
                      Column('course_name', String, nullable=False)
                      )

table_name = 'Spartans'
spartans_table = Table(table_name, metadata,
                       Column('applicant_id', String(50), ForeignKey('Applicants.applicant_id'), nullable=False),
                       Column('trainer_id', Integer, ForeignKey('Trainers.trainer_id'), nullable=False),
                       Column('course_id', Integer, ForeignKey('Courses.course_id'), nullable=False)
                       )

metadata.create_all(engine)

conn.close()