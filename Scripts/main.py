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
# engine = create_engine("mssql+pyodbc://admin:spartaglobal@sparta-global.cjxe5m4vhofo.eu-west-2.rds.amazonaws.com:1433/project?driver=ODBC+Driver+17+for+SQL+Server")
engine = create_engine(connect_string)
print('\rDone!')
# %%
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
    recruiters_table = Table (table_name, metadata,
                            Column('postcode_id', Integer, primary_key=True, autoincrement=True),
                            Column('postcode', String)
                            )
    
    table_name = 'City'
    recruiters_table = Table (table_name, metadata,
                            Column('city_id', Integer, primary_key=True, autoincrement=True),
                            Column('city', String)
                            )
    
    table_name = 'Address'
    recruiters_table = Table (table_name, metadata,
                            Column('address_id', Integer, primary_key=True, autoincrement=True),
                            Column('address', String),
                            Column('postcode_id', Integer, ForeignKey('Postcode.postcode_id')),
                            Column('city_id', Integer, ForeignKey('City.city_id'))   
                            )
    
    table_name = 'University'
    recruiters_table = Table (table_name, metadata,
                            Column('university_id', Integer, primary_key=True, autoincrement=True),
                            Column('university', String)
                            )
    
    table_name = 'Degree'
    recruiters_table = Table (table_name, metadata,
                            Column('degree_id', Integer, primary_key=True, autoincrement=True),
                            Column('degree', String)
                            )
    
    table_name = 'Personal_Details'
    applicants_table = Table(table_name, metadata,
                        Column('person_id', Integer, primary_key=True, autoincrement=True),
                        Column('name', String),
                        Column('gender', String),
                        Column('dob', Date),
                        Column('email', String),
                        Column('phone_number', String),
                        Column('address_id', Integer, ForeignKey('Address.address_id')),
                        Column('degree_id', Integer, ForeignKey('Degree.degree_id')),
                        Column('university_id', Integer, ForeignKey('University.university_id'))                
                        )

    table_name = 'Recruiter'
    recruiters_table = Table (table_name, metadata,
                            Column('recruiter_id', Integer, primary_key=True, autoincrement=True),
                            Column('recruiter_name', String)
                            )

    table_name = 'Applicant'
    applicants_table = Table(table_name, metadata,
                        Column('applicant_id', Integer, primary_key=True, autoincrement=True),
                        Column('invited_date', Date),
                        Column('person_id', Integer, ForeignKey('Personal_Details.person_id')),
                        Column('recruiter_id', Integer, ForeignKey('Recruiter.recruiter_id'))                  
                        )
    
    table_name = 'Academy_Location'
    Academy_location_table = Table (table_name, metadata,
                        Column('academy_location_id', Integer, primary_key=True, autoincrement=True),
                        Column('location_name', String)
                        )
    
    table_name = 'Sparta_Day'
    Academy_location_table = Table (table_name, metadata,
                        Column('sparta_day_id', Integer, primary_key=True, autoincrement=True),
                        Column('sparta_day_date', String),
                        Column('academy_location_id', Integer, ForeignKey('Academy_Location.academy_location_id'))
                        )
    
    table_name = 'Sparta_Day_Result'
    Academy_location_table = Table (table_name, metadata,
                        Column('sparta_day_result_id', Integer, primary_key=True, autoincrement=True),
                        Column('psychometric_result', Integer),
                        Column('presentation_result', Integer),
                        Column('sparta_day_id', Integer, ForeignKey('Sparta_Day.sparta_day_id'))
                        )
    
    table_name = 'Stream'
    Academy_location_table = Table (table_name, metadata,
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
                                Column('stream_id', Integer, ForeignKey('Stream.stream_id')),
                                Column('geo_flex', Boolean),
                                Column('sparta_day_result_id', Integer, ForeignKey('Sparta_Day_Result.sparta_day_result_id'))
                                )

    table_name = 'Strength'
    strengths_table = Table(table_name, metadata,
                        Column('strength_id', Integer, primary_key=True, autoincrement=True),
                        Column('strength', String(30), unique=True, nullable=False)
                        )

    table_name = 'Weakness'
    strengths_table = Table(table_name, metadata,
                        Column('weakness_id', Integer, primary_key=True, autoincrement=True),
                        Column('weakness', String(30), unique=True, nullable=False)
                        )

    table_name = 'Strength_Junction'
    strengths_junction = Table(table_name,metadata,
                            Column('talent_id', Integer, ForeignKey('Talent.talent_id')),
                            Column('strength_id', Integer, ForeignKey('Strength.strength_id'))
                            )

    table_name = 'Weakness_Junction'
    weaknesses_junction = Table(table_name,metadata,
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
    trainers_table = Table(table_name, metadata,
                            Column('trainer_id', String(30), primary_key=True),
                            Column('trainer', String(50), nullable=False, unique=True)
                            )

    table_name = 'Course'
    courses_table = Table(table_name, metadata,
                        Column('course_id', Integer, primary_key=True),
                        Column('stream_id', Integer, ForeignKey('Stream.stream_id')),
                        Column('course_number', String),
                        Column('start_date', Date)
                        )

    table_name = 'Spartan'
    spartans_table = Table(table_name, metadata,
                        Column('spartan_id', Integer, primary_key=True, autoincrement=True),
                        Column('talent_id', Integer, ForeignKey('Talent.talent_id'), unique=True, nullable=True),
                        Column('trainer_id', String(30), ForeignKey('Trainer.trainer_id'), nullable=False),
                        Column('course_id', Integer, ForeignKey('Course.course_id'), nullable=False),
                        )

    table_name = 'Behaviour'
    behaviours_table = Table(table_name, metadata, 
                            Column('spartan_id', Integer, ForeignKey('Spartan.spartan_id')), 
                            Column('week_number', Integer), 
                            Column('analytic', Integer), 
                            Column('independent', Integer), 
                            Column('imaginative', Integer), 
                            Column('studious', Integer), 
                            Column('professional', Integer), 
                            Column('determined', Integer),
                            PrimaryKeyConstraint('spartan_id', 'week_number')
                            )

    metadata.create_all(engine)
print('\rDone!')
print('Loading and transforming data...')
# %%
import importlib
import TalentTable
importlib.reload(TalentTable)
import Applicant_cleaning
importlib.reload(Applicant_cleaning)

# %%
import importlib
import sparta_day_transformation
importlib.reload(sparta_day_transformation)

# %%
df_sparta_day_result, df_sparta_day, df_academy = sparta_day_transformation.getAllData()

# %%
import importlib
import json_strengths_weaknesses
importlib.reload(json_strengths_weaknesses)
import Technologies
importlib.reload(Technologies)


# These take very long
df_temp = json_strengths_weaknesses.pull_json_from_s3('data-eng-210-final-project', 'Talent')
df_strengths, df_strengths_junction = json_strengths_weaknesses.generate_strengths(df_temp)
df_weaknesses, df_weaknesses_junction = json_strengths_weaknesses.generate_weaknesses(df_temp)
df_tech_self_scores, df_tech_junction = Technologies.technologies()
df_talent, df_academy_locations = TalentTable.Talent()
df_talent['geo_flex'] = df_talent['geo_flex'].map({'Yes': True, 'No':False})
df_talent['financial_support_self'] = df_talent['financial_support_self'].map({'Yes': True, 'No':False})
df_talent['self_development'] = df_talent['self_development'].map({'Yes': True, 'No':False})
df_talent['result'] = df_talent['result'].map({'Pass': 1, 'Fail':0})
df_talent = df_talent.rename(columns={'Date':'date'})

# %%
# Caching talent tables because they take ages, dont use normally
# with open('df_strengths.csv', 'w') as file:
#     df_strengths.to_csv(file)
# with open('df_weaknesses.csv', 'w') as file:
#     df_weaknesses.to_csv(file)
# with open('df_strengths_junction.csv', 'w') as file:
#     df_strengths_junction.to_csv(file)
# with open('df_weaknesses_junction.csv', 'w') as file:
#     df_weaknesses_junction.to_csv(file)
# with open('df_talent.csv', 'w') as file:
#     df_talent.to_csv(file)
# with open('df_academy_locations.csv', 'w') as file:
#     df_academy_locations.to_csv(file)
# with open('df_tech_self_scores.csv', 'w') as file:
#     df_tech_self_scores.to_csv(file)
# with open('df_tech_junction.csv', 'w') as file:
#     df_tech_junction.to_csv(file)

#load from cached csvs, careful with losing data types
# df_talent = pd.read_csv('df_talent.csv',index_col=0)
# df_talent = df_talent.rename(columns={'Date':'date'})
# df_talent.date = pd.to_datetime(df_talent.date)
# df_talent_test = df_talent = pd.read_csv('df_talent.csv',index_col=0)
# df_strengths = pd.read_csv('df_strengths.csv',index_col=0)
# df_strengths_junction = pd.read_csv('df_strengths_junction.csv',index_col=0)
# df_weaknesses = pd.read_csv('df_weaknesses.csv',index_col=0)
# df_weaknesses_junction = pd.read_csv('df_weaknesses_junction.csv',index_col=0)
# df_tech_self_scores = pd.read_csv('df_tech_self_scores.csv',index_col=0)
# df_tech_junction = pd.read_csv('df_tech_junction.csv',index_col=0)
# df_talent = pd.read_csv('df_talent.csv',index_col=0)
# df_talent = df_talent.rename(columns={'Date':'date'})
# df_talent.date = pd.to_datetime(df_talent.date)
# df_academy_locations = pd.read_csv('df_academy_locations.csv',index_col=0)

# Applicant csv data
df_applicants, df_location, df_recruiters = Applicant_cleaning.process_locations()

# %%
#There's multiple jsons of the same person on the same date - this condenses them down and merges information
#(e.g. some duplicates have some of the sparta day scores some don't, this fixes that mostly)
df_talent = df_talent.groupby(['applicant_id'], as_index=False).first()
# %%
#Method to add the applicant ID from Applicants to Talents by matching nearest data
def addApplicantIDToTalent(applicant_record):
     if(pd.isnull(applicant_record['invited_date'])):
          return
     result = df_talent[(df_talent['name'].str.lower() == applicant_record['name'].lower()) & ((df_talent['date'].isnull()) | (df_talent['date'] >= applicant_record['invited_date']))].sort_values(by='date',axis=0,ascending=True)
     if not result.empty:
          df_talent.at[result.index[0],'applicant_id'] = applicant_record['applicant_id']
          #print(talent_record['json_key'])
          #print(result.index[0])df_talent
#Do for every row in Applicant
for index, row in df_applicants.iterrows():
    addApplicantIDToTalent(row)

# Load all data from Academy .csv s
import importlib
import academy_behaviours
importlib.reload(academy_behaviours)

df_courses = academy_behaviours.course.copy()
df_courses = df_courses.drop_duplicates().reset_index(drop=True)
df_trainers = academy_behaviours.trainer.copy()
df_trainers = df_trainers.drop_duplicates().reset_index(drop=True)
df_spartans = academy_behaviours.spartans.copy()
df_behaviours = academy_behaviours.behaviour.copy()
# %%
#Need course names to match people and link to talent
df_spartans['course_name'] = df_spartans['course_id'].replace(df_courses.course_id.tolist(),df_courses.course_name.tolist())

#Method to add the applicant ID from Talents to Spartans by matching nearest course
def addApplicantID(talent_record):
     # if talent_record['date'].isna():
     #      return
     result = df_spartans[(df_spartans['name'] == talent_record['name']) & (df_spartans['course_name'] == talent_record['course_interest']) &
                ((df_spartans['date'] >= talent_record['date']) | (pd.isnull(talent_record['date'])))].sort_values(by='date',axis=0,ascending=True)
     if not result.empty:
          df_spartans.at[result.index[0],'json_key'] = talent_record['json_key']
          #print(talent_record['json_key'])
          #print(result.index[0])
#Do for every row in talent
for index, row in df_talent.iterrows():
    addApplicantID(row)

# Collapse duplicates that have one-sided data
df_talent = df_talent.groupby(['applicant_id'], as_index=False).first()
df_talent[df_talent.duplicated(subset="applicant_id",keep=False)]

print('\rDone!')
print('Inserting into database...')
print('Inserting Recruiters (1/15)...')
# %%
# Insert Recruiters table - NO DEPENDENCIES
with engine.connect() as conn:
    conn.execute(text("SET IDENTITY_INSERT Recruiters ON"))
    result = df_recruiters.to_sql('Recruiters',conn,if_exists='append',index=False)
    conn.execute(text("SET IDENTITY_INSERT Recruiters OFF"))
    conn.commit()

# %%
print('Inserting Applicants (2/15)...')
# Insert Applicants table - DEPENDS ON Recruiters
with engine.connect() as conn:
    #conn.execute(text("SET IDENTITY_INSERT Applicants ON"))
    result = df_applicants.drop(["iddate"],axis=1).to_sql('Applicants',conn,if_exists='append',index=False)
    #conn.execute(text("SET IDENTITY_INSERT Applicants OFF"))
    conn.commit()

# %%
print('Inserting Academy_Locations (3/15)...')
# Insert Academy_Locations table - NO DEPENDENCIES
with engine.connect() as conn:
    # conn.execute(text("SET IDENTITY_INSERT Strengths OFF"))
    conn.execute(text("SET IDENTITY_INSERT Academy_Location ON"))
    result = df_academy.to_sql('Academy_Location',conn,if_exists='append',index=False)
    conn.execute(text("SET IDENTITY_INSERT Academy_Location OFF"))
    conn.commit()

# %%
# Separate talent json_keys that are null as they need to be attributed a custom key
df_test = df_talent.drop(['name'], axis=1)[df_talent.json_key.isna()]
df_test = df_test.reset_index(drop=True)
df_test.json_key = df_test.index + 1e9


# %%
print('Inserting Talents (4/15)...')
# Insert Talent table - DEPENDS ON Academy_Locations AND Applicants
df_talent_null_keys = df_talent.drop(['name'], axis=1)[df_talent.json_key.isna()]
df_talent_null_keys = df_talent_null_keys.reset_index(drop=True)
df_talent_null_keys.json_key = df_talent_null_keys.index + 1e9

with engine.connect() as conn:
    # conn.execute(text("SET IDENTITY_INSERT Talent ON"))
    result = df_talent.drop(['name'], axis=1).dropna(subset="json_key",axis=0).to_sql('Talent',conn,if_exists='append',index=False)
    result = df_talent_null_keys.iloc[2:].to_sql('Talent',conn,if_exists='append',index=False)
    # conn.execute(text("SET IDENTITY_INSERT Talent OFF"))
    conn.commit()

# %%
print('Inserting Strengths (5/15)...')
# Insert Strengths table - NO DEPENDENCIES
with engine.connect() as conn:
    conn.execute(text("SET IDENTITY_INSERT Strengths ON"))
    result = df_strengths.to_sql('Strengths',conn,if_exists='append',index=False)
    conn.execute(text("SET IDENTITY_INSERT Strengths OFF"))
    conn.commit()

# %%
print('Inserting Weaknesses (6/15)...')
# Insert Weaknesses table - NO DEPENDENCIES
with engine.connect() as conn:
    conn.execute(text("SET IDENTITY_INSERT Strengths OFF"))
    conn.execute(text("SET IDENTITY_INSERT Weaknesses ON"))
    result = df_weaknesses.to_sql('Weaknesses',conn,if_exists='append',index=False)
    conn.execute(text("SET IDENTITY_INSERT Weaknesses OFF"))
    conn.commit()

# %%
# Insert Tech_Self_Scores table - NO DEPENDENCIES
print('Inserting Tech_Self_Scores (7/15)...')
with engine.connect() as conn:
    conn.execute(text("SET IDENTITY_INSERT Tech_Self_Scores ON"))
    result = df_tech_self_scores.to_sql('Tech_Self_Scores',conn,if_exists='append',index=False)
    conn.execute(text("SET IDENTITY_INSERT Tech_Self_Scores OFF"))
    conn.commit()

# %%
# Have to eliminate leftover duplicates from people with repeated .jsons - this doesnt actually lose any data
df_strengths_junction = df_strengths_junction.drop(index = df_strengths_junction[~df_strengths_junction['json_key'].isin(df_talent['json_key'])].index)
df_weaknesses_junction = df_weaknesses_junction.drop(index = df_weaknesses_junction[~df_weaknesses_junction['json_key'].isin(df_talent['json_key'])].index)
df_tech_junction = df_tech_junction.drop(index=df_tech_junction[~df_tech_junction['json_key'].astype('float').isin(df_talent['json_key'])].index)
# %%
# Insert Strengths_Junction table - DEPENDS ON Strengths_Junction AND Talent
print('Inserting Strengths_Junction (8/15)...')
with engine.connect() as conn:
    # conn.execute(text("SET IDENTITY_INSERT Weaknesses_junction ON"))
    result = df_strengths_junction.to_sql('Strengths_Junction',conn,if_exists='append',index=False)
    # conn.execute(text("SET IDENTITY_INSERT Weaknesses_junction OFF"))
    conn.commit()
# %%
# Insert Weaknesses_Junction table - DEPENDS ON Weaknesses AND Talent
print('Inserting Weaknesses_Junction (9/15)...')
with engine.connect() as conn:
    # conn.execute(text("SET IDENTITY_INSERT Weaknesses_junction ON"))
    result = df_weaknesses_junction.to_sql('Weaknesses_Junction',conn,if_exists='append',index=False)
    # conn.execute(text("SET IDENTITY_INSERT Weaknesses_junction OFF"))
    conn.commit()
# %%
# Insert Tech_Junction table - DEPENDS ON Weaknesses AND Talent
print('Inserting Tech_Junction (10/15)...')
with engine.connect() as conn:
    # conn.execute(text("SET IDENTITY_INSERT Tech_Junction ON"))
    result = df_tech_junction.to_sql('Tech_Junction',conn,if_exists='append',index=False)
    # conn.execute(text("SET IDENTITY_INSERT Tech_Junction OFF"))
    conn.commit()
# %%
# Insert Location table - DEPENDS ON Applicants
print('Inserting Location (11/15)...')
with engine.connect() as conn:
    conn.execute(text("SET IDENTITY_INSERT Location ON"))
    result = df_location.to_sql('Location',conn,if_exists='append',index=False)
    conn.execute(text("SET IDENTITY_INSERT Location OFF"))
    conn.commit()
# %%
# Insert Courses table - NO DEPENDENCIES
print('Inserting Courses (12/15)...')
with engine.connect() as conn:
    #conn.execute(text("SET IDENTITY_INSERT Courses ON"))
    result = df_courses.to_sql('Courses',conn,if_exists='append',index=False)
    #conn.execute(text("SET IDENTITY_INSERT Courses OFF"))
    conn.commit()
# %%
# Insert Trainers table - NO DEPENDENCIES
print('Inserting Trainers (13/15)...')
with engine.connect() as conn:
    #conn.execute(text("SET IDENTITY_INSERT Trainers ON"))
    df_trainers.to_sql('Trainers',conn,if_exists='append',index=False)
    #conn.execute(text("SET IDENTITY_INSERT Trainers OFF"))
    conn.commit()
# %%
# Insert Spartans table - DEPENDS ON Courses AND Tables
print('Inserting Spartans (14/15)...')
with engine.connect() as conn:
    df_spartans.drop(['behaviour_id','name','course_name','date'],axis=1).to_sql('Spartans',conn,if_exists='append',index=False)
# %%
#cast for merge
df_spartans.json_key = df_spartans.json_key.astype('int')
# %%
#Need to query database for auto-generated spartan ids, merge behaviours so that we can extract the sparta_id into behaviour records
with engine.connect() as conn:
    df_spartans_temp = pd.read_sql(text("SELECT * FROM Spartans"), conn)
    df_behaviours_insert = pd.merge(df_behaviours, pd.merge(df_spartans, df_spartans_temp, on='json_key')[
        ['behaviour_id', 'spartan_id']], on='behaviour_id').drop(['behaviour_id', 'name'], axis=1).rename(columns={'week': 'week_number'})

# %%
# Insert Behaviours table - DEPENDS ON Spartans
print('Inserting Behaviours (15/15)...')
with engine.connect() as conn:
    # conn.execute(text("SET IDENTITY_INSERT Behaviours ON"))
    df_behaviours_insert.to_sql('Behaviours', conn, if_exists='append', index=False)
    # conn.execute(text("SET IDENTITY_INSERT Behaviours OFF"))
    conn.commit()

print('Data insertion complete!')