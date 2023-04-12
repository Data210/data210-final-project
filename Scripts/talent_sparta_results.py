import boto3
import json
import pandas as pd
import io
pd.set_option('display.max_columns', None)
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

# Select relevant columns
df_table = df_all[['name','date','self_development','financial_support_self','result','course_interest','geo_flex','json_key']]
df_table = df_table.reset_index(drop=True)

# Open sparta_day_clean.csv
df_sparta_day_clean = pd.read_csv('sparta_day_clean.csv')
df_table['name'] = df_table['name'].str.upper()

# Make name columns the same format
df_sparta_day_clean['name'] = df_sparta_day_clean['FirstName'] + ' ' + df_sparta_day_clean['LastName']

# Join the dataFrames
df_t = pd.merge(df_table, df_sparta_day_clean, on='name')

# Select relevant columns
df_clean = df_t[['SpartaDayTalentID','date','self_development','financial_support_self','result','course_interest','Psychometrics','Presentation','geo_flex','Academy']]



df_clean['academyid'] = pd.factorize(df_clean['Academy'])[0]
academy_table = df_clean[['academyid', 'Academy']].drop_duplicates().reset_index(drop=True)
df_clean = df_clean.drop(columns=['Academy'])

df_clean.rename(columns={"SpartaDayTalentID": "application_id", "academyid": "academy_location_id", "Psychometrics": "psycometric_results", "Presentation": "presentation_results"}, inplace=True)

academy_table.rename(columns={"academyid": "academy_location_id", "Academy": "location_name"}, inplace=True)

Talent = df_clean
Talent.reset_index(drop=True, inplace=True)


Academy_Locations = academy_table
Academy_Locations.reset_index(drop=True, inplace=True)

# Write to new file
with open('Talent.csv','w') as file:
    Talent.to_csv(file)

# Write to new file
with open('Academy_Locations.csv','w') as file:
    Academy_Locations.to_csv(file)

