import pandas as pd
import boto3
import json
from io import StringIO
import sqlalchemy as db
from dotenv import load_dotenv
import os

load_dotenv()  # Load environment variables from .env file

db_password = os.getenv("DB_PASSWORD")

engine = db.create_engine(f"mssql+pyodbc://sa:{db_password}@localhost:1433/Talent?driver=ODBC+Driver+17+for+SQL+Server")
connection = engine.connect()

# Set up S3 client
s3 = boto3.client('s3')

# Define the S3 bucket name and file name
bucket_name = 'data-eng-210-final-project'
prefix = 'Talent/'

# Initialize empty list to store strength data
strength_data = []

# Iterate over all JSON files in S3 bucket with given prefix
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

for obj in response['Contents']:
    key = obj['Key']
    if key.endswith('.json'):
        '''
        Read JSON file from S3 into DataFrame
        '''
        obj = s3.get_object(Bucket=bucket_name, Key=key)
        body = obj['Body'].read().decode('utf-8')
        data = json.loads(body)
        df = pd.json_normalize(data)

        '''
        Extract strength data and append to list
        '''
        strengths = df[['id', 'strength']].dropna()
        strengths['strengths'] = strengths['strengths'].apply(lambda x: x.split(','))  # Split comma-separated values
        strengths = strengths.explode('strengths')  # Explode the column to create one row per strength
        strength_data.append(strengths)

# Concatenate all strength data into a single DataFrame
strength_df = pd.concat(strength_data, ignore_index=True)

# Insert strength data into SQL Server
strength_df.to_sql('Strengths', connection, if_exists='append', index=False)

connection.close()
