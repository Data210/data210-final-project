import pandas as pd
import boto3
import json
import string
from io import StringIO

# Set up S3 client
s3 = boto3.client('s3')

# Define the S3 bucket name and file name
bucket_name = 'data-eng-210-final-project'
prefix = 'Talent/'

# Initialize empty list to store DataFrames
json_df = []

# Iterate over all JSON files in S3 bucket with given prefix
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

# The code uses s3.list_objects_v2 to retrieve a list of objects from the S3 bucket with the given prefix.
# the code then loops through the list of objects and checks if the key name ends with ".json".
for obj in response['Contents']:
    key = obj['Key']
    if key.endswith('.json'):
        # Read JSON file from S3 into DataFrame
        # If the key ends with ".json", the code uses s3.get_object to read the contents of the file into a Python string.
        # It then uses json.loads to convert the string into a Python dictionary, and
        # pd.json_normalize to convert the dictionary into a Pandas DataFrame.
        obj = s3.get_object(Bucket=bucket_name, Key=key)
        body = obj['Body'].read().decode('utf-8')
        data = json.loads(body)
        try:
            df = pd.json_normalize(data)
        except ValueError:
            print(f"Unable to normalize data from {key}")
            continue

        # Append DataFrame to list
        json_df.append(df)

# Concatenate all DataFrames into a single DataFrame
df = pd.concat(json_df, ignore_index=True)

# Replace double slashes with single slashes in the 'date' column
df['date'] = df['date'].str.replace('//', '/')

# Add sparta_day_person_id
df["applicant_id"] = df["name"].str.lower() + pd.to_datetime(df['date'], dayfirst=True).dt.strftime('%Y%m%d')
df["applicant_id"] = df["applicant_id"].str.replace('[ '+string.punctuation+']', '', regex=True)

df = df.set_index('applicant_id')
df = df.reset_index()

print(df.head())

# use explode function to split the list column into multiple rows
df_strengths = df[['applicant_id', 'strengths']].explode('strengths')
df_strengths = df_strengths.set_index('applicant_id')
df_strengths = df_strengths.reset_index()

strengths = df_strengths['strengths'].unique()
# Create a new dataframe with strength_id and strength columns
df_strengths_junction = pd.DataFrame({'strength_id': range(len(strengths)),
                            'strength': strengths})

print(df_strengths_junction.head())
with open('df_strengths_junction.csv','w') as file:
    df_strengths_junction.to_csv(file)

df_strenghts_joined = pd.merge(df_strengths, df_strengths_junction,left_on='strengths', right_on='strength', how='left')
#print(df_strenghts_joined.head(50))

df_strength_id = df_strenghts_joined[['applicant_id','strength_id']]
print(df_strength_id.head())
with open('df_strength_id.csv','w') as file:
    df_strength_id.to_csv(file)



# use explode function to split the list column into multiple rows
df_weaknesses = df[['applicant_id', 'weaknesses']].explode('weaknesses')
df_weaknesses = df_weaknesses.set_index('applicant_id')
df_weaknesses = df_weaknesses.reset_index()
print(df_weaknesses.head())
# with open('weaknesses_json.csv','w') as file:
#     df_weaknesses.to_csv(file)

weaknesses = df_weaknesses['weaknesses'].unique()
# Create a new dataframe with strength_id and strength columns
df_weaknesses_junction = pd.DataFrame({'weakness_id': range(len(weaknesses)),
                            'weakness': weaknesses})

print(df_weaknesses_junction.head())
with open('df_weaknesses_junction.csv','w') as file:
    df_weaknesses_junction.to_csv(file)

df_weaknesses_joined = pd.merge(df_weaknesses, df_weaknesses_junction,left_on='weaknesses', right_on='weakness', how='left')
#print(df_strenghts_joined.head(50))

df_weakness_id = df_weaknesses_joined[['applicant_id','weakness_id']]
print(df_weakness_id.head())
with open('df_weakness_id.csv','w') as file:
    df_weakness_id.to_csv(file)