from s3 import S3Client
import pandas as pd
import re
import string
from datetime import datetime, timezone

# Config
bucket_name = "data-eng-210-final-project"
client = S3Client()


def getAllData() -> pd.DataFrame:
    """
    Returns a DataFrame with ALL the Sparta Day records in the bucket
    """
    file_keys = []
    # Find all files with .json extension in bucket
    for item in client.getAllObjects(bucket_name).filter(Prefix='Talent'):
        if item.key.endswith('.json'):
            file_keys.append(item.key)
    return getData(file_keys)


def getDataSince(dt: datetime):
    """
    Returns a DataFrame with the Sparta Day records modified at or after the provided datetime
    """
    dt = dt.replace(tzinfo=timezone.utc)
    file_keys = []
    for item in client.getAllObjects(bucket_name).filter(Prefix='Talent'):
        if item.key.endswith('.json') and item.last_modified > dt:
            file_keys.append(item.key)
    return getData(file_keys)


def getData(keys: list) -> pd.DataFrame:
    """
    Returns a DataFrame with all the records in files from the list of keys provided
    """
    # Create empty df and loop through all files, parsing and concatenating to main df
    # Set column names
    df = pd.DataFrame()
    for key in keys:
        json_object = client.getJSON(bucket_name,key)
        temp_df = parseFile(json_object)
        temp_df['json_key'] = key.split('Talent/')[1].split('.json')[0]
        df = pd.concat([df,temp_df])

    return df

# Save results to a file
def getAllDataAsCSV(filename='sparta_day_clean.csv'):
    """
    Writes ALL the related records in the bucket to a .csv
    """
    with open(filename, 'w') as file:
        getAllData().to_csv(file)

# Utility function for parsing the file
def parseFile(json_object: str) -> pd.DataFrame:
    """
    Takes in a JSON object from the Talent .json files and retains a DataFrame with the record data
    """
    df = pd.json_normalize(json_object)
    return df
