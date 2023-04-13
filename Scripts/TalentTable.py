

import sparta_day_transformation

import boto3
import json
import pandas as pd
import io

s3 = boto3.resource('s3')
bucket_name = 'data-eng-210-final-project'
bucket = s3.Bucket('data-eng-210-final-project')

# Initalize DataFrame
df_all=pd.DataFrame()

# Put all jsons into a single DataFrame with the json key
for obj in bucket.objects.all():
    if obj.key.endswith('.json'):
        x = obj.key
        obj = s3.Object(bucket_name, x)
        json_data = obj.get()['Body'].read().decode('utf-8')
        data = json.loads(json_data)
        df = pd.json_normalize(data)
        
        text = x
        result = text.split('Talent/')[1].split('.json')[0]
        df['json_key'] = result
        
        df_all = pd.concat([df_all, df])

df = sparta_day_transformation.getAllData()

df_table = df_all[['name','date','self_development','financial_support_self','result','course_interest','geo_flex']]

df_table['date'] = df_table['date'].str.replace('//', '/')
df_table['date'] = pd.to_datetime(df_table['date'], format='%d/%m/%Y')

# convert the datetime values to a string with the desired format
df_table['date'] = df_table['date'].dt.strftime('%Y%m%d')

df_table['name'] = df_table['name'].str.replace(' ', '')
df_table['name'] = df_table['name'].str.lower()
df_table.head()

df_table['SpartaDayTalentID'] = df_table['name'].str.cat(df_table['date'], sep='')

df_t = pd.merge(df_table, df, on='SpartaDayTalentID')

df_clean = df_t[['SpartaDayTalentID','date','self_development','financial_support_self','result','course_interest','Psychometrics','Presentation','geo_flex','Academy']]

df_clean['academyid'] = pd.factorize(df_clean['Academy'])[0]
academy_table = df_clean[['academyid', 'Academy']].drop_duplicates().reset_index(drop=True)
df_clean = df_clean.drop(columns=['Academy'])

df_clean.rename(columns={"SpartaDayTalentID": "applicant_id", "academyid": "academy_location_id", "Psychometrics": "psycometric_results", "Presentation": "presentation_results"}, inplace=True)
academy_table.rename(columns={"academyid": "academy_location_id", "Academy": "location_name"}, inplace=True)

Talent = df_clean
Talent.reset_index(drop=True, inplace=True)
Talent.set_index('applicant_id', inplace=True)

Academy_Locations = academy_table
Academy_Locations.reset_index(drop=True, inplace=True)
Academy_Locations.set_index('academy_location_id', inplace=True)

#print(Talent.head())

#print(Academy_Locations.head())