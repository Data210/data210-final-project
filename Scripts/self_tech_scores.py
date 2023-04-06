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

df_tech_scores = df_all.filter(regex='^tech_self_score')
df_tech_scores['name'] = df_all['name']

df_tech_scores.columns = df_tech_scores.columns.str.replace("tech_self_score.", "")
df_melted = pd.melt(df_tech_scores, id_vars='name', value_vars=df_tech_scores.columns[:-1], var_name='language', value_name='score')
df_melted = df_melted.dropna()
df_melted = df_melted.sort_values(by='name')
df_melted = df_melted.reset_index()
df_melted = df_melted[['name', 'language', 'score']]

with open('tech_self_scores.csv','w') as file:
    df_melted.to_csv(file)