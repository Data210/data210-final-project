# %%
from s3 import S3Client
import pandas as pd
import re
import string
import io
from datetime import datetime, timezone
from dateutil.parser import parse
from create_database import create_database
from sqlalchemy import text, Table, create_engine
import utilities
import sqlalchemy as db
import sys

print(sys.argv)

try:
    filepath = sys.argv[1]
except:
    filepath = ''

# %%
engine = create_database()

# %%
import talent_transformation as Talent
import applicant_transformation as Applicants
import sparta_day_transformation as SpartaDay
import academy_transformation as Academy
# %%
df_sparta_day_result, df_sparta_day, df_academy = SpartaDay.getAllData(filepath)

# %%
df_spartan, df_course, df_stream, df_trainer, df_behaviour = Academy.getAllData(filepath)

# %%
df_talent, df_strength_junction, df_weakness_junction, df_tech_junction,  df_strength, df_weakness, df_tech = Talent.getAllData(filepath)

# %%
df_applicant, df_personal_details, df_uni, df_degree, df_address ,df_postcode, df_city,  df_recruiter = Applicants.process_locations(engine,filepath)

# %%
#Add new streams if only inserting Talent with new streams
from utilities import checkNewRecords
with engine.connect() as conn:
    current_stream_df = pd.read_sql(text(
        """
        SELECT * FROM Stream
        """), conn)
df_stream_from_talent = df_talent[['stream_id']].drop_duplicates().rename(columns={'stream_id':'stream'})
df_stream_from_talent = checkNewRecords(df_stream_from_talent, pd.concat([df_stream,current_stream_df]),'stream_id')
df_stream = pd.concat([df_stream,df_stream_from_talent])

# %%
df_applicant_with_names = pd.merge(df_applicant, df_personal_details[['person_id','name']])
df_applicant_insert = pd.merge(df_applicant_with_names,df_sparta_day_result[['sparta_day_result_id','name','sparta_day_date']],
                               left_on=['name','invited_date'],
                               right_on=['name','sparta_day_date'],
                               how='left')\
                                .drop(['name','sparta_day_date'],axis=1)

# %%
with engine.connect() as conn:
    current_applicant_with_names_df = pd.read_sql(text(
        """
        SELECT a.applicant_id, a.invited_date, pd.name FROM Applicant as a
        JOIN Personal_Details as pd
        ON a.person_id = pd.person_id
        WHERE a.invited_date IS NOT NULL
        AND a.applicant_id NOT IN (SELECT t.applicant_id FROM Talent.dbo.Talent as t WHERE t.applicant_id IS NOT NULL)
        """), conn)
    
df_talent.date = pd.to_datetime(df_talent.date)
df_applicant_with_names.invited_date = pd.to_datetime(df_applicant_with_names.invited_date)
current_applicant_with_names_df.invited_date = pd.to_datetime(current_applicant_with_names_df.invited_date)

df_talent_insert = pd.merge_asof(
    df_talent.sort_values('date'),
    pd.concat([current_applicant_with_names_df,df_applicant_with_names]).dropna(subset=['invited_date'], how='any')\
    .sort_values('invited_date')[['applicant_id', 'name', 'invited_date']],
    left_on='date',
    right_on='invited_date',
    by='name',
    direction='forward').drop(['name', 'invited_date', 'date'], axis=1)

df_talent_insert.stream_id = df_talent_insert.stream_id.map(dict(pd.concat([df_stream,current_stream_df])[['stream','stream_id']].values.tolist()))

# %%
with engine.connect() as conn:
    current_talent_df = pd.read_sql(text(
        """
        SELECT t.talent_id, pd.name, t.stream_id, sd.sparta_day_date as date FROM Talent as t
        JOIN Applicant as ap on t.applicant_id = ap.applicant_id
        JOIN Sparta_Day_Result as sdr on sdr.sparta_day_result_id = ap.sparta_day_result_id
        JOIN Sparta_Day as sd on sd.sparta_day_id = sdr.sparta_day_id
        JOIN Personal_Details as pd on pd.person_id = ap.person_id
        WHERE t.talent_id NOT IN (SELECT talent_id FROM Spartan)
        AND t.pass = 1
        """), conn)

current_talent_df['stream_id'] = current_talent_df[['stream_id']].astype('int64')
current_talent_df['date'] = pd.to_datetime(current_talent_df['date'])
df_course.start_date = pd.to_datetime(df_course.start_date)
df_course.stream_id = df_course.stream_id.astype('int64')
df_stream.stream_id = df_stream.stream_id.astype('int64')

df_talent_temp = df_talent.copy()
df_talent_temp.stream_id = df_talent_temp.stream_id.map(dict(pd.concat([df_stream,current_stream_df])[['stream','stream_id']].values.tolist()))
df_talent_temp.stream_id = df_talent_temp.stream_id.astype('int64')

df_spartan_insert = pd.merge_asof(
    pd.merge(df_spartan,df_course[['stream_id','course_id','start_date']]).sort_values('start_date'),
    pd.concat([current_talent_df,df_talent_temp[['name','date','stream_id','talent_id']]]).sort_values('date'),
    left_on='start_date',
    right_on='date',
    left_by=['name','stream_id'],
    right_by=['name','stream_id'],
    direction='backward').drop(['name', 'start_date', 'date','stream_id'], axis=1)

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

# Insert City table - NO DEPENDENCIES
    print('Inserting City (2/23)...')
    conn.execute(text("SET IDENTITY_INSERT City ON"))
    result = df_city.to_sql('City',conn,if_exists='append',index=False)
    conn.execute(text("SET IDENTITY_INSERT City OFF"))
    conn.commit()

# Insert Address table - DEPENDS ON Postcode AND City
    print('Inserting Address (3/23)...')
    conn.execute(text("SET IDENTITY_INSERT Address ON"))
    result = df_address.to_sql('Address',conn,if_exists='append',index=False)
    conn.execute(text("SET IDENTITY_INSERT Address OFF"))
    conn.commit()

# Insert Degree table - NO DEPENDENCIES
    print('Inserting Degree (4/23)...')
    conn.execute(text("SET IDENTITY_INSERT Degree ON"))
    result = df_degree.to_sql('Degree',conn,if_exists='append',index=False)
    conn.execute(text("SET IDENTITY_INSERT Degree OFF"))
    conn.commit()

# Insert University table - NO DEPENDENCIES
    print('Inserting University (5/23)...')
    conn.execute(text("SET IDENTITY_INSERT University ON"))
    result = df_uni.to_sql('University',conn,if_exists='append',index=False)
    conn.execute(text("SET IDENTITY_INSERT University OFF"))
    conn.commit()

# Insert Personal Details table - DEPENDS ON Address AND University AND Degree
    print('Inserting Personal Details (6/23)...')
    conn.execute(text("SET IDENTITY_INSERT Personal_Details ON"))
    result = df_personal_details.to_sql('Personal_Details',conn,if_exists='append',index=False)
    conn.execute(text("SET IDENTITY_INSERT Personal_Details OFF"))
    conn.commit()

# Insert Recruiter table - NO DEPENDENCIES
    print('Inserting Recruiter (7/23)...')
    conn.execute(text("SET IDENTITY_INSERT Recruiter ON"))
    result = df_recruiter.to_sql('Recruiter',conn,if_exists='append',index=False)
    conn.execute(text("SET IDENTITY_INSERT Recruiter OFF"))
    conn.commit()

# Insert Academy_Location table - NO DEPENDENCIES
    print('Inserting Academy_Location (8/23)...')
    conn.execute(text("SET IDENTITY_INSERT Academy_Location ON"))
    result = df_academy.to_sql('Academy_Location',conn,if_exists='append',index=False)
    conn.execute(text("SET IDENTITY_INSERT Academy_Location OFF"))
    conn.commit()

# Insert Sparta_Day table - NO DEPENDENCIES
    print('Inserting Sparta_Day (9/23)...')
    conn.execute(text("SET IDENTITY_INSERT Sparta_Day ON"))
    result = df_sparta_day.to_sql('Sparta_Day',conn,if_exists='append',index=False)
    conn.execute(text("SET IDENTITY_INSERT Sparta_Day OFF"))
    conn.commit()

# Insert Sparta Day Result table - DEPENDS ON Recruiters AND Personal_Details
    print('Inserting Sparta Day Result (10/23)...')
    conn.execute(text("SET IDENTITY_INSERT Sparta_Day_Result ON"))
    result = df_sparta_day_result.drop(['name','sparta_day_date'],axis=1)\
        .to_sql('Sparta_Day_Result',conn,if_exists='append',index=False)
    conn.execute(text("SET IDENTITY_INSERT Sparta_Day_Result OFF"))
    conn.commit()
# Update Applicant
    current_df = pd.read_sql(text("""SELECT ap.applicant_id, pd.name, ap.invited_date, ap.sparta_day_result_id FROM Applicant as ap
                                    JOIN Personal_Details as pd on ap.person_id = pd.person_id
                                    WHERE ap.invited_date IS NOT NULL AND
                                    ap.sparta_day_result_id IS NULL"""), conn)
    current_df.invited_date = pd.to_datetime(current_df.invited_date)
    df_sparta_day_result.sparta_day_date = pd.to_datetime(df_sparta_day_result.sparta_day_date)
    df_applicant_with_sparta_day = pd.merge(current_df[['applicant_id','name','invited_date']], df_sparta_day_result[['sparta_day_result_id','name','sparta_day_date']],
                                        left_on=['name','invited_date'],
                                        right_on=['name','sparta_day_date'],
                                        how='left')\
                                        .drop(['name','sparta_day_date','invited_date'],axis=1)
    
    for index, row in df_applicant_with_sparta_day.dropna(subset='sparta_day_result_id').iterrows():
        result = conn.execute(text(f"UPDATE Applicant SET sparta_day_result_id = {row['sparta_day_result_id']} WHERE applicant_id = {row['applicant_id']}"))
    conn.commit()
# Insert Applicant table - DEPENDS ON Recruiters AND Personal_Details AND Sparta_Day_Result
    print('Inserting Applicant (11/23)...')
    conn.execute(text("SET IDENTITY_INSERT Applicant ON"))
    result = df_applicant_insert.to_sql('Applicant',conn,if_exists='append',index=False)
    conn.execute(text("SET IDENTITY_INSERT Applicant OFF"))
    conn.commit()

# Insert Stream table - NO DEPENDENCIES
    print('Inserting Stream (12/23)...')
    conn.execute(text("SET IDENTITY_INSERT Stream ON"))
    result = df_stream.to_sql('Stream',conn,if_exists='append',index=False)
    conn.execute(text("SET IDENTITY_INSERT Stream OFF"))
    conn.commit()

# Insert Talent table - DEPENDS ON Applicant AND Stream
    print('Inserting Talent (13/23)...')
    conn.execute(text("SET IDENTITY_INSERT Talent ON"))
    result = df_talent_insert.to_sql('Talent',conn,if_exists='append',index=False)
    conn.execute(text("SET IDENTITY_INSERT Talent OFF"))
    conn.commit()

# Insert Strengths table - NO DEPENDENCIES
    print('Inserting Strength (14/23)...')
    conn.execute(text("SET IDENTITY_INSERT Strength ON"))
    result = df_strength.to_sql('Strength',conn,if_exists='append',index=False)
    conn.execute(text("SET IDENTITY_INSERT Strength OFF"))
    conn.commit()

# Insert Weakness table - NO DEPENDENCIES
    print('Inserting Weakness (15/23)...')
    conn.execute(text("SET IDENTITY_INSERT Weakness ON"))
    result = df_weakness.to_sql('Weakness',conn,if_exists='append',index=False)
    conn.execute(text("SET IDENTITY_INSERT Weakness OFF"))
    conn.commit()

# Insert Technology table - NO DEPENDENCIES
    print('Inserting Technology (16/23)...')
    conn.execute(text("SET IDENTITY_INSERT Technology ON"))
    result = df_tech.to_sql('Technology',conn,if_exists='append',index=False)
    conn.execute(text("SET IDENTITY_INSERT Technology OFF"))
    conn.commit()

# Insert Strength_Junction table - DEPENDS ON Strengths_Junction AND Talent
    print('Inserting Strength_Junction (17/23)...')
    # conn.execute(text("SET IDENTITY_INSERT Weaknesses_junction ON"))
    result = df_strength_junction.to_sql('Strength_Junction',conn,if_exists='append',index=False)
    # conn.execute(text("SET IDENTITY_INSERT Weaknesses_junction OFF"))
    conn.commit()

# Insert Weaknesses_Junction table - DEPENDS ON Weaknesses AND Talent
    print('Inserting Weakness_Junction (18/23)...')
    # conn.execute(text("SET IDENTITY_INSERT Weakness_Junction ON"))
    result = df_weakness_junction.to_sql('Weakness_Junction',conn,if_exists='append',index=False)
    # conn.execute(text("SET IDENTITY_INSERT Weakness_Junction OFF"))
    conn.commit()

# Insert Tech_Junction table - DEPENDS ON Weaknesses AND Talent
    print('Inserting Tech_Junction (19/23)...')
    # conn.execute(text("SET IDENTITY_INSERT Tech_Junction ON"))
    result = df_tech_junction.to_sql('Tech_Junction',conn,if_exists='append',index=False)
    # conn.execute(text("SET IDENTITY_INSERT Tech_Junction OFF"))
    conn.commit()

# Insert Course table - DEPENDS ON Stream
    print('Inserting Course (20/23)...')
    conn.execute(text("SET IDENTITY_INSERT Course ON"))
    result = df_course.to_sql('Course',conn,if_exists='append',index=False)
    conn.execute(text("SET IDENTITY_INSERT Course OFF"))
    conn.commit()

# Insert Trainers table - NO DEPENDENCIES
    print('Inserting Trainer (21/23)...')
    conn.execute(text("SET IDENTITY_INSERT Trainer ON"))
    df_trainer.to_sql('Trainer',conn,if_exists='append',index=False)
    conn.execute(text("SET IDENTITY_INSERT Trainer OFF"))
    conn.commit()

# Insert Spartans table - DEPENDS ON Courses AND Tables
    print('Inserting Spartan (22/23)...')
    conn.execute(text("SET IDENTITY_INSERT Spartan ON"))
    df_spartan_insert.to_sql('Spartan',conn,if_exists='append',index=False)
    conn.execute(text("SET IDENTITY_INSERT Spartan OFF"))
    conn.commit()

# Insert Behaviours table - DEPENDS ON Spartans
    print('Inserting Behaviour (23/23)...')
    # conn.execute(text("SET IDENTITY_INSERT Behaviour ON"))
    df_behaviour.to_sql('Behaviour', conn, if_exists='append', index=False)
    # conn.execute(text("SET IDENTITY_INSERT Behaviour OFF"))
    conn.commit()

print('Data insertion complete!')


