from s3 import S3Client
import pandas as pd
import re
import string
from datetime import datetime, timezone
import importlib
from utilities import checkNewRecords, splitAndRemap

from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from connection_string import create_connection_string



load_dotenv()  # Load environment variables from .env file
connect_string = create_connection_string()
engine = create_engine(connect_string)

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
    sparta_day_result_df = pd.DataFrame()
    txts,pool_keys = client.getObjectsPooled(keys, bucket_name, 'txt')
    for key, file_txt in zip(pool_keys, txts):
        #text = client.getCSV(bucket_name, key)
        sparta_day_result_df = pd.concat(
            [sparta_day_result_df, pd.DataFrame(parseTextFile(file_txt))])
    # Set column names
    sparta_day_result_df.columns = ["name", "psychometric_result", "presentation_result", "sparta_day_date", "academy"]

    with engine.connect() as conn:
        current_academy_df = pd.read_sql(text("SELECT * FROM Academy_Location"), conn)
        current_sparta_day_df = pd.read_sql(text("SELECT * FROM Sparta_Day"), conn)
        current_sparta_day_df.sparta_day_date = pd.to_datetime(current_sparta_day_df.sparta_day_date)
        current_sparta_day_result_df = pd.read_sql(
            text("""SELECT sdr.*, pd.name FROM Sparta_Day_Result AS sdr 
            JOIN Applicant AS a
            ON sdr.sparta_day_result_id = a.sparta_day_result_id
            JOIN Personal_Details as pd
            ON a.person_id = pd.person_id
            """), conn)
        
    # Additional cleaning, strip whitespace, enforce data types
    sparta_day_result_df.name = sparta_day_result_df.name.str.strip(" ")
    sparta_day_result_df.psychometric_result = sparta_day_result_df.psychometric_result.astype(int)
    sparta_day_result_df.presentation_result = sparta_day_result_df.presentation_result.astype(int)
    sparta_day_result_df.sparta_day_date = pd.to_datetime(sparta_day_result_df.sparta_day_date)
    sparta_day_result_df.academy = sparta_day_result_df.academy.replace({'London':'Leeds'})

    # Drop any local duplicates, defer index creating after database cross check
    sparta_day_result_df = sparta_day_result_df[['name','psychometric_result','presentation_result','sparta_day_date','academy']]
    sparta_day_df = sparta_day_result_df[['sparta_day_date','academy']].drop_duplicates()

    # Check for duplicates against current Academy_Location table
    # academy_df becomes the diff, pd.concat() current + new for mapping
    academy_df = sparta_day_df[['academy']].drop_duplicates()
    academy_df = checkNewRecords(academy_df,current_academy_df,'academy_id')

    sparta_day_df = splitAndRemap(sparta_day_df,pd.concat([current_academy_df[['academy','academy_id']],academy_df]),'academy')
    sparta_day_df = checkNewRecords(sparta_day_df,current_sparta_day_df,'sparta_day_id')

    # Map sparta_day_id back, needs to happen before duplicate check as it is identifying information
    sparta_day_result_df = splitAndRemap(sparta_day_result_df,
                                           pd.merge(
                                                pd.concat([sparta_day_df[['sparta_day_date','sparta_day_id','academy_id']],
                                                current_sparta_day_df[['sparta_day_date','sparta_day_id','academy_id']]]),
                                                pd.concat([current_academy_df[['academy','academy_id']],academy_df])
                                            ),
                                            ['academy_id','academy']
                                        )
    # sparta_day_result_df = pd.merge(sparta_day_result_df,
    #                                 pd.merge(pd.concat([sparta_day_df[['sparta_day_date','sparta_day_id','academy_id']],
    #                                                     current_sparta_day_df[['sparta_day_date','sparta_day_id','academy_id']]]),
    #                                                     pd.concat([current_academy_df[['academy','academy_id']],academy_df]))
    # ).drop(['academy_id'],axis=1)

    # Duplicate check
    sparta_day_result_df = checkNewRecords(sparta_day_result_df,current_sparta_day_result_df,'sparta_day_result_id')
    
    return sparta_day_result_df, sparta_day_df, academy_df

# Save results to a file


def getAllDataAsCSV(filename='sparta_day_clean.csv'):
    """
    Writes ALL the related records in the bucket to a .csv
    """
    with open(filename, 'w') as file:
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
            print("Error in line:",line)
        names = names.replace(',', '')
        names = list(map(lambda x: x.title(), names.split(" ")))
        names = " ".join(names)
        columns = (names + ',' + rest).split(',')
        row = [columns[0], re.search('([0-9]{1,3})/', columns[-2]).group(
            1), re.search('([0-9]{1,3})/', columns[-1]).group(1)]+[split[0], split[1].split(' ')[0]]
        table.append(row)
    return table
