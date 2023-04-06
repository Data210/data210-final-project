import boto3
import json
import pandas as pd
import io
pd.set_option('display.max_columns', None)
s3 = boto3.resource('s3')
bucket_name = 'data-eng-210-final-project'
bucket = s3.Bucket('data-eng-210-final-project')

df_all=pd.DataFrame()

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

df_table = df_all[['name','date','self_development','financial_support_self','result','course_interest','geo_flex','json_key']]
df_table = df_table.reset_index(drop=True)


df_sparta_day_clean = pd.read_csv('sparta_day_clean.csv')
df_table['name'] = df_table['name'].str.upper()

df_sparta_day_clean['name'] = df_sparta_day_clean['FirstName'] + ' ' + df_sparta_day_clean['LastName']

df_t = pd.merge(df_table, df_sparta_day_clean, on='name')

df_clean = df_t[['json_key','name','date','self_development','financial_support_self','result','course_interest','geo_flex','Psychometrics','Presentation','Academy']]

with open('talent_sparta_results.csv','w') as file:
    df_clean.to_csv(file)