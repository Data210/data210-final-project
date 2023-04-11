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
    sparta_day_file_keys = []
    # Find all files with .txt extension in bucket
    for item in client.getAllObjects(bucket_name).filter(Prefix='Talent'):
        if item.key.endswith('.txt'):
            sparta_day_file_keys.append(item.key)
    return getData(sparta_day_file_keys)


def getDataSince(dt: datetime):
    """
    Returns a DataFrame with the Sparta Day records modified at or after the provided datetime
    """
    dt = dt.replace(tzinfo=timezone.utc)
    sparta_day_file_keys = []
    for item in client.getAllObjects(bucket_name).filter(Prefix='Talent'):
        if item.key.endswith('.txt') and item.last_modified > dt:
            sparta_day_file_keys.append(item.key)
    return getData(sparta_day_file_keys)


def getData(keys: list) -> pd.DataFrame:
    """
    Returns a DataFrame with all the records in files from the list of keys provided
    """
    # Create empty df and loop through all files, parsing and concatenating to main df
    # Set column names
    sparta_day_df = pd.DataFrame()
    for key in keys:
        text = client.getCSV(bucket_name,key)
        sparta_day_df = pd.concat([sparta_day_df,pd.DataFrame(parseTextFile(text))])
    # Set column names
    sparta_day_df.columns = ["FirstName","LastName","Psychometrics","Presentation","Date","Academy"]

    # Additional cleaning, strip whitespace, enforce data types
    sparta_day_df.FirstName = sparta_day_df.FirstName.str.strip(" ")
    sparta_day_df.LastName = sparta_day_df.LastName.str.strip(" ")
    sparta_day_df.Psychometrics = sparta_day_df.Psychometrics.astype(int)
    sparta_day_df.Presentation = sparta_day_df.Presentation.astype(int)
    sparta_day_df.Date = sparta_day_df.Date = pd.to_datetime(sparta_day_df.Date)

    # Add ID
    sparta_day_df["SpartaDayTalentID"] = sparta_day_df.FirstName.str.lower() + sparta_day_df.LastName.str.lower() + sparta_day_df.Date.dt.strftime('%Y%m%d')
    sparta_day_df["SpartaDayTalentID"] = sparta_day_df["SpartaDayTalentID"].str.replace('[ '+string.punctuation+']', '', regex=True)

    return sparta_day_df

# Save results to a file
def getAllDataAsCSV():
    """
    Writes ALL the records in the bucket to a .csv
    """
    with open('sparta_day_clean.csv', 'w') as file:
        getAllData().to_csv(file)

# Utility function for parsing the sparta day .txt format
def parseTextFile(text: str) -> list:
    """
    Takes in a string from the Sparta Day .txt files' body and returns a list of rows, each row is a list split by column
    """
    split = text.replace('\r', '').split('\n')
    table = []
    for line in split[3:-1]:
        try:
            names, rest = line.rsplit("-", 1)
        except:
            print(line)
        names = names.replace(',', '')
        columns = (names + ',' + rest).split(',')
        row = columns[0].split(" ", 1)+[re.search('([0-9]{1,3})/', columns[-2]).group(
            1), re.search('([0-9]{1,3})/', columns[-1]).group(1)]+[split[0], split[1].split(' ')[0]]
        table.append(row)
    return table
