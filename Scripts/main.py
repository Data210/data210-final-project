# %%
import sys
# caution: path[0] is reserved for script path (or '' in REPL)
#sys.path.insert(1, 'D:\\Sparta\\final_project\\data210-final-project\\Scripts')
import sqlalchemy as db
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Date, ForeignKey, Boolean, PrimaryKeyConstraint
from sqlalchemy import text
from sqlalchemy_utils import database_exists, create_database
from dotenv import load_dotenv
import os
import pandas as pd
import numpy as np
from connection_string import create_connection_string
load_dotenv()  # Load environment variables from .env file
#db_password = os.getenv("DB_PASSWORD")
connect_string = create_connection_string()
# %%
print('Creating Engine...')
engine = create_engine("mssql+pyodbc://admin:spartaglobal@project-testing.cjxe5m4vhofo.eu-west-2.rds.amazonaws.com:1433/project?driver=ODBC+Driver+17+for+SQL+Server")
#engine = create_engine(connect_string)
print('\rDone!')

# %%
#Create databse if it doesn't exist
try:
    if database_exists(engine.url) == False:
        create_database(engine.url)
except:
    create_database(engine.url)
# drop all tables
metadata = db.MetaData()
metadata.reflect(bind=engine)
metadata.drop_all(bind=engine)
print('Creating Schema...')
with engine.connect() as conn:
    metadata = MetaData()

    #define tables
    table_name = 'Postcode'
    postcode_table = Table (table_name, metadata,
                            Column('postcode_id', Integer, primary_key=True, autoincrement=True),
                            Column('postcode', String)
                            )
    
    table_name = 'City'
    city_table = Table (table_name, metadata,
                            Column('city_id', Integer, primary_key=True, autoincrement=True),
                            Column('city', String)
                            )
    
    table_name = 'Address'
    address_table = Table (table_name, metadata,
                            Column('address_id', Integer, primary_key=True, autoincrement=True),
                            Column('address', String),
                            Column('postcode_id', Integer, ForeignKey('Postcode.postcode_id')),
                            Column('city_id', Integer, ForeignKey('City.city_id'))   
                            )
    
    table_name = 'University'
    university_table = Table (table_name, metadata,
                            Column('uni_id', Integer, primary_key=True, autoincrement=True),
                            Column('uni', String)
                            )
    
    table_name = 'Degree'
    degree_table = Table (table_name, metadata,
                            Column('degree_id', Integer, primary_key=True, autoincrement=True),
                            Column('degree', String)
                            )
    
    table_name = 'Personal_Details'
    personal_details_table = Table(table_name, metadata,
                        Column('person_id', Integer, primary_key=True, autoincrement=True),
                        Column('name', String),
                        Column('gender', String),
                        Column('dob', Date),
                        Column('email', String),
                        Column('phone_number', String),
                        Column('address_id', Integer, ForeignKey('Address.address_id')),
                        Column('degree_id', Integer, ForeignKey('Degree.degree_id')),
                        Column('uni_id', Integer, ForeignKey('University.uni_id'))                
                        )
    
    table_name = 'Recruiter'
    recruiter_table = Table (table_name, metadata,
                            Column('recruiter_id', Integer, primary_key=True, autoincrement=True),
                            Column('recruiter_name', String)
                            )
    
    table_name = 'Applicant'
    applicant_table = Table(table_name, metadata,
                        Column('applicant_id', Integer, primary_key=True, autoincrement=True),
                        Column('invited_date', Date),
                        Column('person_id', Integer, ForeignKey('Personal_Details.person_id')),
                        Column('recruiter_id', Integer, ForeignKey('Recruiter.recruiter_id')),
                        Column('sparta_day_result_id', Integer, ForeignKey('Sparta_Day_Result.sparta_day_result_id'))                
                        )
    
    table_name = 'Academy_Location'
    Academy_location_table = Table (table_name, metadata,
                        Column('academy_id', Integer, primary_key=True, autoincrement=True),
                        Column('academy', String)
                        )
    
    table_name = 'Sparta_Day'
    Sparta_Day_table = Table (table_name, metadata,
                        Column('sparta_day_id', Integer, primary_key=True, autoincrement=True),
                        Column('sparta_day_date', String),
                        Column('academy_id', Integer, ForeignKey('Academy_Location.academy_id'))
                        )
    
    table_name = 'Sparta_Day_Result'
    Sparta_Day_Result_table = Table (table_name, metadata,
                        Column('sparta_day_result_id', Integer, primary_key=True, autoincrement=True),
                        Column('psychometric_result', Integer),
                        Column('presentation_result', Integer),
                        Column('sparta_day_id', Integer, ForeignKey('Sparta_Day.sparta_day_id'))
                        )
    
    table_name = 'Stream'
    Stream_table = Table (table_name, metadata,
                        Column('stream_id', Integer, primary_key=True, autoincrement=True),
                        Column('stream', String)
                        )
    table_name = 'Talent'
    talent_table = Table(table_name, metadata,
                                Column('talent_id', Integer, primary_key=True),
                                Column('applicant_id', Integer, ForeignKey('Applicant.applicant_id')),
                                Column('self_development', Boolean),
                                Column('financial_support_self', Boolean),
                                Column('pass', Boolean),
                                Column('stream_id', Integer,ForeignKey('Stream.stream_id')),
                                Column('geo_flex', Boolean)
                                )
    table_name = 'Strength'
    strength_table = Table(table_name, metadata,
                        Column('strength_id', Integer, primary_key=True, autoincrement=True),
                        Column('strength', String(30), unique=True, nullable=False)
                        )
    table_name = 'Weakness'
    weakness_table = Table(table_name, metadata,
                        Column('weakness_id', Integer, primary_key=True, autoincrement=True),
                        Column('weakness', String(30), unique=True, nullable=False)
                        )
    table_name = 'Strength_Junction'
    strength_junction = Table(table_name,metadata,
                            Column('talent_id', Integer, ForeignKey('Talent.talent_id')),
                            Column('strength_id', Integer, ForeignKey('Strength.strength_id'))
                            )
    table_name = 'Weakness_Junction'
    weakness_junction = Table(table_name,metadata,
                            Column('talent_id', Integer, ForeignKey('Talent.talent_id')),
                            Column('weakness_id', Integer)
                            )
    table_name = 'Technology'
    tech_table = Table(table_name, metadata,
                    Column('tech_id', Integer, primary_key=True, autoincrement=True),
                    Column('tech_name', String(50), unique=True, nullable=False)
                    )
    table_name = 'Tech_Junction'
    tech_junction_table = Table(table_name, metadata,
                                Column('tech_id', Integer, ForeignKey("Technology")),
                                Column('talent_id', Integer, ForeignKey('Talent.talent_id')),
                                Column('score', Integer, nullable=False)
                                )
    table_name = 'Trainer'
    trainer_table = Table(table_name, metadata,
                            Column('trainer_id', String(30), primary_key=True),
                            Column('trainer', String(50), nullable=False, unique=True)
                            )
    table_name = 'Course'
    course_table = Table(table_name, metadata,
                        Column('course_id', Integer, primary_key=True),
                        Column('stream_id', Integer, ForeignKey('Stream.stream_id')),
                        Column('course_num', String),
                        Column('start_date', Date)
                        )
    table_name = 'Spartan'
    spartan_table = Table(table_name, metadata,
                        Column('spartan_id', Integer, primary_key=True, autoincrement=True),
                        Column('talent_id', Integer, ForeignKey('Talent.talent_id'), unique=True, nullable=True),
                        Column('trainer_id', String(30), ForeignKey('Trainer.trainer_id'), nullable=False),
                        Column('course_id', Integer, ForeignKey('Course.course_id'), nullable=False),
                        )
    table_name = 'Behaviour'
    behaviour_table = Table(table_name, metadata, 
                            Column('spartan_id', Integer, ForeignKey('Spartan.spartan_id')), 
                            Column('week', Integer), 
                            Column('analytic', Integer), 
                            Column('independent', Integer), 
                            Column('imaginative', Integer), 
                            Column('studious', Integer), 
                            Column('professional', Integer), 
                            Column('determined', Integer),
                            PrimaryKeyConstraint('spartan_id', 'week')
                            )
    metadata.create_all(engine)
print('\rDone!')
print('Loading and transforming data...')

# %% [markdown]
# Import transformation scripts

# %%
# %%
import talent_transformation as Talent
import applicant_transformation as Applicants
import sparta_day_transformation as SpartaDay
import academy_transformation as Academy

# %%
import importlib
importlib.reload(Talent)
importlib.reload(Applicants)
importlib.reload(SpartaDay)
importlib.reload(Academy)

# %% [markdown]
# Load in all the data

# %%
df_sparta_day_result, df_sparta_day, df_academy = SpartaDay.getAllData()
df_spartan, df_course, df_stream, df_trainer, df_behaviour = Academy.getAllData()
df_talent, df_strength_junction, df_weakness_junction, df_tech_junction,  df_strength, df_weakness, df_tech = Talent.getAllData()
df_applicant, df_personal_details, df_uni, df_degree, df_address ,df_postcode, df_city,  df_recruiter = Applicants.process_locations()

# %% [markdown]
# Link Applicant to Sparta Day Result by matching name and invited_date

# %%
df_applicant_with_names = pd.merge(df_applicant, df_personal_details[['person_id','name']])
df_applicant_insert = pd.merge(df_applicant_with_names,df_sparta_day_result[['sparta_day_result_id','name','date']],
                               left_on=['name','invited_date'],
                               right_on=['name','date'],
                               how='left')\
                                .drop(['name','date'],axis=1)

# %% [markdown]
# Link Applicant to Talent by matching name and approximate invited_date (to deal with input error)

# %%
df_talent_insert = pd.merge_asof(
    df_talent.sort_values('date'),
    df_applicant_with_names.dropna(subset=['invited_date'], how='any')\
    .sort_values('invited_date')[['applicant_id', 'name', 'invited_date']],
    left_on='date',
    right_on='invited_date',
    by='name',
    direction='forward').drop(['name', 'invited_date', 'date'], axis=1)

df_talent_insert.stream_id = df_talent_insert.stream_id.map(dict(zip(df_stream.stream.to_list(),df_stream.stream_id.to_list())))


# %% [markdown]
# Link Spartan to Talent by matching name, stream_id and approximate invited_date (to deal with input error)

# %%
df_talent_temp = df_talent.copy()
df_talent_temp.stream_id = df_talent_temp.stream_id.map(dict(zip(df_stream.stream.to_list(),df_stream.stream_id.to_list())))

df_spartan_insert = pd.merge_asof(
    pd.merge(df_spartan,df_course[['stream_id','course_id','start_date']]).sort_values('start_date'),
    df_talent_temp[['name','date','stream_id','talent_id']].sort_values('date'),
    left_on='start_date',
    right_on='date',
    left_by=['name','stream_id'],
    right_by=['name','stream_id'],
    direction='backward').drop(['name', 'start_date', 'date','stream_id'], axis=1)


# %% [markdown]
# Caching talent tables because they take ages, dont use normally

# %%
# with open('df_strengths.csv', 'w') as file:
#     df_strengths.to_csv(file)
# with open('df_weaknesses.csv', 'w') as file:
#     df_weaknesses.to_csv(file)
# with open('df_strength_junction.csv', 'w') as file:
#     df_strength_junction.to_csv(file)
# with open('df_weakness_junction.csv', 'w') as file:
#     df_weakness_junction.to_csv(file)
# with open('df_talent.csv', 'w') as file:
#     df_talent.to_csv(file)
# with open('df_academy_locations.csv', 'w') as file:
#     df_academy_locations.to_csv(file)
# with open('df_tech.csv', 'w') as file:
#     df_tech.to_csv(file)
# with open('df_tech_junction.csv', 'w') as file:
#     df_tech_junction.to_csv(file)

# %% [markdown]
# Load from cached csvs, careful with losing data types

# %%
# df_talent = pd.read_csv('df_talent.csv',index_col=0)
# df_talent = df_talent.rename(columns={'Date':'date'})
# df_talent.date = pd.to_datetime(df_talent.date)
# df_talent_test = df_talent = pd.read_csv('df_talent.csv',index_col=0)
# df_strengths = pd.read_csv('df_strengths.csv',index_col=0)
# df_strength_junction = pd.read_csv('df_strength_junction.csv',index_col=0)
# df_weaknesses = pd.read_csv('df_weaknesses.csv',index_col=0)
# df_weakness_junction = pd.read_csv('df_weakness_junction.csv',index_col=0)
# df_tech = pd.read_csv('df_tech.csv',index_col=0)
# df_tech_junction = pd.read_csv('df_tech_junction.csv',index_col=0)
# df_talent = pd.read_csv('df_talent.csv',index_col=0)
# df_talent = df_talent.rename(columns={'Date':'date'})
# df_talent.date = pd.to_datetime(df_talent.date)
# df_academy_locations = pd.read_csv('df_academy_locations.csv',index_col=0)

# %% [markdown]
# Begin inserting into database

# %%
print('\rDone!')
print('Inserting into database...')
print('Inserting Postcode (1/23)...')
# Insert Postcode table - NO DEPENDENCIES
with engine.connect() as conn:
    conn.execute(text("SET IDENTITY_INSERT Postcode ON"))
    result = df_postcode.to_sql('Postcode',conn,if_exists='append',index=False)
    conn.execute(text("SET IDENTITY_INSERT Postcode OFF"))
    conn.commit()

# %%
# Insert City table - NO DEPENDENCIES
print('Inserting City (2/23)...')
with engine.connect() as conn:
    conn.execute(text("SET IDENTITY_INSERT City ON"))
    result = df_city.to_sql('City',conn,if_exists='append',index=False)
    conn.execute(text("SET IDENTITY_INSERT City OFF"))
    conn.commit()

# %%
# Insert Address table - DEPENDS ON Postcode AND City
print('Inserting Address (3/23)...')
with engine.connect() as conn:
    conn.execute(text("SET IDENTITY_INSERT Address ON"))
    result = df_address.to_sql('Address',conn,if_exists='append',index=False)
    conn.execute(text("SET IDENTITY_INSERT Address OFF"))
    conn.commit()

# %%
# Insert Degree table - NO DEPENDENCIES
print('Inserting Degree (4/23)...')
with engine.connect() as conn:
    conn.execute(text("SET IDENTITY_INSERT Degree ON"))
    result = df_degree.to_sql('Degree',conn,if_exists='append',index=False)
    conn.execute(text("SET IDENTITY_INSERT Degree OFF"))
    conn.commit()

# %%
# Insert University table - NO DEPENDENCIES
print('Inserting University (5/23)...')
with engine.connect() as conn:
    conn.execute(text("SET IDENTITY_INSERT University ON"))
    result = df_uni.to_sql('University',conn,if_exists='append',index=False)
    conn.execute(text("SET IDENTITY_INSERT University OFF"))
    conn.commit()

# %%
# Insert Personal Details table - DEPENDS ON Address AND University AND Degree
print('Inserting Personal Details (6/23)...')
with engine.connect() as conn:
    conn.execute(text("SET IDENTITY_INSERT Personal_Details ON"))
    result = df_personal_details.to_sql('Personal_Details',conn,if_exists='append',index=False)
    conn.execute(text("SET IDENTITY_INSERT Personal_Details OFF"))
    conn.commit()

# %%
# Insert Recruiter table - NO DEPENDENCIES
print('Inserting Recruiter (7/23)...')
with engine.connect() as conn:
    conn.execute(text("SET IDENTITY_INSERT Recruiter ON"))
    result = df_recruiter.to_sql('Recruiter',conn,if_exists='append',index=False)
    conn.execute(text("SET IDENTITY_INSERT Recruiter OFF"))
    conn.commit()

# %%
# Insert Academy_Location table - NO DEPENDENCIES
print('Inserting Academy_Location (8/23)...')
with engine.connect() as conn:
    conn.execute(text("SET IDENTITY_INSERT Academy_Location ON"))
    result = df_academy.to_sql('Academy_Location',conn,if_exists='append',index=False)
    conn.execute(text("SET IDENTITY_INSERT Academy_Location OFF"))
    conn.commit()

# %%
# Insert Sparta_Day table - NO DEPENDENCIES
print('Inserting Sparta_Day (9/23)...')
with engine.connect() as conn:
    conn.execute(text("SET IDENTITY_INSERT Sparta_Day ON"))
    result = df_sparta_day.to_sql('Sparta_Day',conn,if_exists='append',index=False)
    conn.execute(text("SET IDENTITY_INSERT Sparta_Day OFF"))
    conn.commit()

# %%
# Insert Sparta Day Result table - DEPENDS ON Recruiters AND Personal_Details
print('Inserting Sparta Day Result (10/23)...')
with engine.connect() as conn:
    conn.execute(text("SET IDENTITY_INSERT Sparta_Day_Result ON"))
    result = df_sparta_day_result.drop(['name','date','academy'],axis=1)\
        .to_sql('Sparta_Day_Result',conn,if_exists='append',index=False)
    conn.execute(text("SET IDENTITY_INSERT Sparta_Day_Result OFF"))
    conn.commit()

# %%
# Insert Applicant table - DEPENDS ON Recruiters AND Personal_Details AND Sparta_Day_Result
print('Inserting Applicant (11/23)...')
with engine.connect() as conn:
    conn.execute(text("SET IDENTITY_INSERT Applicant ON"))
    result = df_applicant_insert.to_sql('Applicant',conn,if_exists='append',index=False)
    conn.execute(text("SET IDENTITY_INSERT Applicant OFF"))
    conn.commit()

# %%
# Insert Stream table - NO DEPENDENCIES
print('Inserting Stream (12/23)...')
with engine.connect() as conn:
    conn.execute(text("SET IDENTITY_INSERT Stream ON"))
    result = df_stream.to_sql('Stream',conn,if_exists='append',index=False)
    conn.execute(text("SET IDENTITY_INSERT Stream OFF"))
    conn.commit()

# %%
# Insert Talent table - DEPENDS ON Applicant AND Stream
print('Inserting Talent (13/23)...')
with engine.connect() as conn:
    conn.execute(text("SET IDENTITY_INSERT Talent ON"))
    result = df_talent_insert.to_sql('Talent',conn,if_exists='append',index=False)
    conn.execute(text("SET IDENTITY_INSERT Talent OFF"))
    conn.commit()

# %%
# Insert Strengths table - NO DEPENDENCIES
print('Inserting Strength (14/23)...')
with engine.connect() as conn:
    conn.execute(text("SET IDENTITY_INSERT Strength ON"))
    result = df_strength.to_sql('Strength',conn,if_exists='append',index=False)
    conn.execute(text("SET IDENTITY_INSERT Strength OFF"))
    conn.commit()

# %%
# Insert Weakness table - NO DEPENDENCIES
print('Inserting Weakness (15/23)...')
with engine.connect() as conn:
    conn.execute(text("SET IDENTITY_INSERT Weakness ON"))
    result = df_weakness.to_sql('Weakness',conn,if_exists='append',index=False)
    conn.execute(text("SET IDENTITY_INSERT Weakness OFF"))
    conn.commit()

# %%
# Insert Technology table - NO DEPENDENCIES
print('Inserting Technology (16/23)...')
with engine.connect() as conn:
    conn.execute(text("SET IDENTITY_INSERT Technology ON"))
    result = df_tech.to_sql('Technology',conn,if_exists='append',index=False)
    conn.execute(text("SET IDENTITY_INSERT Technology OFF"))
    conn.commit()

# %%
# Insert Strength_Junction table - DEPENDS ON Strengths_Junction AND Talent
print('Inserting Strength_Junction (17/23)...')
with engine.connect() as conn:
    # conn.execute(text("SET IDENTITY_INSERT Weaknesses_junction ON"))
    result = df_strength_junction.to_sql('Strength_Junction',conn,if_exists='append',index=False)
    # conn.execute(text("SET IDENTITY_INSERT Weaknesses_junction OFF"))
    conn.commit()

# %%
# Insert Weaknesses_Junction table - DEPENDS ON Weaknesses AND Talent
print('Inserting Weakness_Junction (18/23)...')
with engine.connect() as conn:
    # conn.execute(text("SET IDENTITY_INSERT Weakness_Junction ON"))
    result = df_weakness_junction.to_sql('Weakness_Junction',conn,if_exists='append',index=False)
    # conn.execute(text("SET IDENTITY_INSERT Weakness_Junction OFF"))
    conn.commit()


# %%
# Insert Tech_Junction table - DEPENDS ON Weaknesses AND Talent
print('Inserting Tech_Junction (19/23)...')
with engine.connect() as conn:
    # conn.execute(text("SET IDENTITY_INSERT Tech_Junction ON"))
    result = df_tech_junction.to_sql('Tech_Junction',conn,if_exists='append',index=False)
    # conn.execute(text("SET IDENTITY_INSERT Tech_Junction OFF"))
    conn.commit()


# %%
# Insert Course table - DEPENDS ON Stream
print('Inserting Course (20/23)...')
with engine.connect() as conn:
    conn.execute(text("SET IDENTITY_INSERT Course ON"))
    result = df_course.to_sql('Course',conn,if_exists='append',index=False)
    conn.execute(text("SET IDENTITY_INSERT Course OFF"))
    conn.commit()

# %%
# Insert Trainers table - NO DEPENDENCIES
print('Inserting Trainer (21/23)...')
with engine.connect() as conn:
    #conn.execute(text("SET IDENTITY_INSERT Trainer ON"))
    df_trainer.to_sql('Trainer',conn,if_exists='append',index=False)
    #conn.execute(text("SET IDENTITY_INSERT Trainer OFF"))
    conn.commit()

# %%
# Insert Spartans table - DEPENDS ON Courses AND Tables
print('Inserting Spartan (22/23)...')
with engine.connect() as conn:
    conn.execute(text("SET IDENTITY_INSERT Spartan ON"))
    df_spartan_insert.to_sql('Spartan',conn,if_exists='append',index=False)
    conn.execute(text("SET IDENTITY_INSERT Spartan OFF"))
    conn.commit()

# %%
# Insert Behaviours table - DEPENDS ON Spartans
print('Inserting Behaviour (23/23)...')
with engine.connect() as conn:
    # conn.execute(text("SET IDENTITY_INSERT Behaviour ON"))
    df_behaviour.to_sql('Behaviour', conn, if_exists='append', index=False)
    # conn.execute(text("SET IDENTITY_INSERT Behaviour OFF"))
    conn.commit()

# %%
print('Data insertion complete!')


