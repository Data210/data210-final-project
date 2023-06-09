import boto3
import json
import pandas as pd
import io
pd.set_option('display.max_columns', None)
s3 = boto3.resource('s3')
bucket_name = 'data-eng-210-final-project'
bucket = s3.Bucket('data-eng-210-final-project')

df_all=pd.DataFrame()

# Get all JSONs into a dataframe
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

# Read sparta_day_clean.csv
df_sparta_day_clean = pd.read_csv('sparta_day_clean.csv')
df_all['name'] = df_all['name'].str.upper()
df_sparta_day_clean['name'] = df_sparta_day_clean['FirstName'] + ' ' + df_sparta_day_clean['LastName']

# Merge the dataframes
df_t = pd.merge(df_all, df_sparta_day_clean, on='name')

# Extract tech self scores
filtered_df = df_t.filter(regex='^tech_self_score|^SpartaDayTalentID$')
filtered_df.columns = filtered_df.columns.str.replace("tech_self_score.", "")

# Melt the dataframe
df = filtered_df.melt(id_vars='SpartaDayTalentID', value_vars=filtered_df.columns[:-1], var_name='languages', value_name='scores')
df = df.dropna()
df = df.sort_values(by='SpartaDayTalentID')
df['language_id'] = pd.factorize(df['languages'])[0]
df=df.reset_index(drop=True)

# Split into language table and self score table
language_table = df[['languages', 'language_id']].drop_duplicates().reset_index(drop=True)
tech_self_scores = df[['SpartaDayTalentID','language_id','scores']]
tech_self_scores.rename(columns={"SpartaDayTalentID": "applicationid", "scores": "score"})
language_table.rename(columns={"languages": "language"})

# Write them into a csv
with open('tech_self_scores.csv','w') as file:
    tech_self_scores.to_csv(file)

with open('language_table.csv','w') as file:
    language_table.to_csv(file)