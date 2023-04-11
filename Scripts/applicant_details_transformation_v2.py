from s3 import S3Client
import pandas as pd
import re
import string
import io
from datetime import datetime, timezone

# Config
bucket_name = "data-eng-210-final-project"
client = S3Client()


def getAllData() -> pd.DataFrame:
    """
    Returns a DataFrame with ALL the Sparta Day records in the bucket
    """
    applicants_file_keys = []
    # Find all files with .txt extension in bucket
    for item in client.getAllObjects(bucket_name).filter(Prefix='Talent'):
        if item.key.endswith('.csv'):
            applicants_file_keys.append(item.key)
    return getData(applicants_file_keys)


def getDataSince(dt: datetime):
    """
    Returns a DataFrame with the Applicants records modified at or after the provided datetime
    """
    dt = dt.replace(tzinfo=timezone.utc)
    applicants_file_keys = []
    for item in client.getAllObjects(bucket_name).filter(Prefix='Talent'):
        if item.key.endswith('.csv') and item.last_modified > dt:
            applicants_file_keys.append(item.key)
    return getData(applicants_file_keys)


def getData(keys: list) -> pd.DataFrame:
    """
    Returns a DataFrame with all the records in files from the list of keys provided
    """
    # Create empty df and loop through all files, parsing and concatenating to main df
    # Set column names
    df = pd.DataFrame()
    for key in keys:
        text = client.getCSV(bucket_name, key)
        df = pd.concat(
            [df, pd.DataFrame(parseFile(text))])

    return df

def getAllDataAsCSV(filename = 'applicants_clean.csv'):
    """
    Writes ALL the related records in the bucket to a .csv
    """
    with open(filename, 'w') as file:
        getAllData().to_csv(file)

def parseFile(text: str) -> pd.DataFrame:
    """
    Takes the Applicants .csv files' body and returns a Pandas DataFrame with the data
    """
    # Read text body into csv
    df = pd.read_csv(io.StringIO(text))
    # Clean phone number - no brackets, separated by dashes
    df['phone_number'] = df['phone_number'].str.replace(
        '(', '').str.replace(')', '').str.replace(' ', '-')
    # Enforce datetime type
    df['dob'] = pd.to_datetime(df['dob'], format='%d/%m/%Y')
    # Replace invited_date column with the full date
    # First add day to the date
    df['invited_date'] = df['invited_date'].astype(
        'Int64', errors='raise').apply(str) + ' ' + df['month']
    # Convert the mixed date formats to datetime
    df['invited_date'] = pd.to_datetime(
        df['invited_date'], errors='raise', dayfirst=True, format='mixed')
    
    
    df['year']=df['month'].str[-4:]
    df['month to use']=df['month']
    df['month to use']=df['month to use'].str[:-5]
    #this is a pain but necessary
    df.loc[ df['month to use'] == 'JAN', 'month to use'] = '01'
    df.loc[ df['month to use'] == 'JANUARY', 'month to use'] = '01'
    df.loc[ df['month to use'] == 'FEB', 'month to use'] = '02'
    df.loc[ df['month to use'] == 'FEBRUARY', 'month to use'] = '02'
    df.loc[ df['month to use'] == 'MARCH', 'month to use'] = '03'
    df.loc[ df['month to use'] == 'APRIL', 'month to use'] = '04'
    df.loc[ df['month to use'] == 'MAY', 'month to use'] = '05'
    df.loc[ df['month to use'] == 'JUNE', 'month to use'] = '06'
    df.loc[ df['month to use'] == 'JULY', 'month to use'] = '07'
    df.loc[ df['month to use'] == 'AUGUST', 'month to use'] = '08'
    df.loc[ df['month to use'] == 'AUG', 'month to use'] = '08'
    df.loc[ df['month to use'] == 'SEPT', 'month to use'] = '09'
    df.loc[ df['month to use'] == 'SEPTEMBER', 'month to use'] = '09'
    df.loc[ df['month to use'] == 'OCT', 'month to use'] = '10'
    df.loc[ df['month to use'] == 'OCTOBER', 'month to use'] = '10'
    df.loc[ df['month to use'] == 'NOV', 'month to use'] = '11'
    df.loc[ df['month to use'] == 'NOVEMBER', 'month to use'] = '11'
    df.loc[ df['month to use'] == 'DEC', 'month to use'] = '12'
    df.loc[ df['month to use'] == 'DECEMBER', 'month to use'] = '12'
    # format is YYYYMMDD
    df['iddate']= df['year']+df['month to use']+df['invited_date']
    
    
    #fix format
    df['iddate'] = df['iddate'].astype(str)
    df['iddate']=df['iddate'].str.replace(".0", "")
    
    #format 1 to 01 etc.
    for id in range(0,len(df['iddate'])):
        if len(df['iddate'][id]) ==7:
           word=df['iddate'][id]
           last_letter=df['iddate'][id][-1]
           word=word[:-1]
            df['iddate'][id]=word+'0'+last_letter

    #handle nulls
    for date in range(0, len(df['iddate'])):
     if pd.isnull(df.loc[date, 'iddate']):
           predate= date-1
           df['iddate'][date]=df['iddate'][predate]
            df['iddate'][date] =df['iddate'][date][:-2]
            df['iddate'][date] = df['iddate'][date]+"00"
    
    #drop intermediate columns
    df=df.drop(['month','year','month to use'],axis=1)
      # Create unique ID from lowercase name joined without spaces and invited_date in %Y%m%d format
       df['uniqueid'] = df['name'].str.lower().str.replace('[ '+string.punctuation+']',
                                                            '', regex=True) + df['iddate]
       
    return df
