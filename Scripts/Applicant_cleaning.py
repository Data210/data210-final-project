from s3 import S3Client
import pandas as pd
import re
import string
import io
from datetime import datetime, timezone
from dateutil.parser import parse
pd.options.mode.chained_assignment = None
pd.set_option('display.max_columns', None)
# Config
bucket_name = "data-eng-210-final-project"
client = S3Client()

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
    df['invited_date'] = pd.to_datetime(df['invited_date'], errors='raise', dayfirst=True, format='mixed')
    # Create unique ID from lowercase name joined without spaces and invited_date in %Y%m%d format
    df['iddate']=df['invited_date'].dt.strftime('%Y%m%d')
    df['iddate'] = df['iddate'].astype(str)
    df['iddate'] = df['iddate'].str.replace(".0", "")
    for date in range(1, len(df['iddate'])):
        if df.loc[date, 'iddate'] == "nan":
            predate= date -1
            df['iddate'][date]=df['iddate'][predate]
            df['iddate'][date] =df['iddate'][date][:-2]
            df['iddate'][date] = df['iddate'][date]+"00"
    df['applicant_id'] = df['name'].str.lower().str.replace('[ '+string.punctuation+']',
                                                        '', regex=True) + df['iddate']
    # Drop month column as found in invited_date column
    df = df.drop(columns=['month','id'])

    return df

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


def getAllData() -> pd.DataFrame:

    applicants_file_keys = []
    # Find all files with .txt extension in bucket
    for item in client.getAllObjects(bucket_name).filter(Prefix='Talent'):
        if item.key.endswith('.csv'):
            applicants_file_keys.append(item.key)
    return getData(applicants_file_keys)


def getAllDataAsCSV(filename = 'applicants_clean.csv'):
    """
    Writes ALL the related records in the bucket to a .csv
    """
    with open(filename, 'w') as file:
        getAllData().to_csv(file)

def recruit() -> pd.DataFrame:
    main_df = getAllData()
    #fix recruiter names, case-by-case basis unfortunately
    main_df['invited_by']=main_df['invited_by'].replace('Bruno Belbrook','Bruno Bellbrook')
    main_df['invited_by'] = main_df['invited_by'].replace('Fifi Etton', 'Fifi Eton')

    unique_recruiters = main_df['invited_by'].unique()
    recruiter_ids = {name: i + 1 for i, name in enumerate(unique_recruiters)}

    # Replace the 'invited_by' column with 'recruiter_id'
    main_df['recruiter_id'] = main_df['invited_by'].map(recruiter_ids)

    # Create a dataframe of unique recruiters and their IDs
    recruiters_df = pd.DataFrame(list(recruiter_ids.items()), columns=['recruiter_name', 'recruiter_id'])

    return main_df, recruiters_df

def process_locations() -> pd.DataFrame:
    main_df, recruiter_df = recruit()
    # Create a new dataframe 'location_df' with unique values of 'address', 'postcode', 'city', and 'applicant_id'
    location_df = main_df[['address', 'postcode', 'city', 'applicant_id']].drop_duplicates()
    location_df['location_id'] = range(len(location_df))
    main_df.drop(['address', 'postcode', 'city','invited_by'], axis=1, inplace=True)
    # Return all dataframes
    return main_df, location_df, recruiter_df

#this is final method needed for sql, unless we need to write csv

def process_data_since(dt: datetime):

    main_df=getDataSince(dt)
    if not main_df.empty:
        main_df['invited_by'] = main_df['invited_by'].replace('Bruno Belbrook', 'Bruno Bellbrook')
        main_df['invited_by'] = main_df['invited_by'].replace('Fifi Etton', 'Fifi Eton')

        unique_recruiters = main_df['invited_by'].unique()
        recruiter_ids = {name: i + 1 for i, name in enumerate(unique_recruiters)}

        # Replace the 'invited_by' column with 'recruiter_id'
        main_df['recruiter_id'] = main_df['invited_by'].map(recruiter_ids)

        # Create a dataframe of unique recruiters and their IDs
        recruiter_df = pd.DataFrame(list(recruiter_ids.items()), columns=['recruiter_name', 'recruiter_id'])

        location_df = main_df[['address', 'postcode', 'city', 'applicant_id']].drop_duplicates()
        location_df['location_id'] = range(len(location_df))
        main_df.drop(['address', 'postcode', 'city','invited_by'], axis=1, inplace=True)
        # Return all dataframes
        return main_df, location_df, recruiter_df
    else:
        return "No New Records."

def Update_Data(time):
    dt=parse(time)
    output= applicant_details_transformation_v2.process_data_since(dt)

    if len(output) == 3:
        main_df, location_df, recruiter_df = output
        return main_df, location_df, recruiter_df
    elif len(output) == 15:
        print(output)
    else:
        print("Unexpected")