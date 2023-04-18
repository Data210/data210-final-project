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
    df['invited_date'] = pd.to_datetime(
        df['invited_date'], errors='raise', dayfirst=True, format='mixed')
    # Drop month column as found in invited_date column
    df = df.drop(columns=['month', 'id'])

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


def getAllDataAsCSV(filename='applicants_clean.csv'):
    """
    Writes ALL the related records in the bucket to a .csv
    """
    with open(filename, 'w') as file:
        getAllData().to_csv(file)


def recruit() -> pd.DataFrame:
    main_df = getAllData()
    # fix recruiter names, case-by-case basis unfortunately
    main_df['invited_by'] = main_df['invited_by'].replace(
        'Bruno Belbrook', 'Bruno Bellbrook')
    main_df['invited_by'] = main_df['invited_by'].replace(
        'Fifi Etton', 'Fifi Eton')

    unique_recruiters = main_df['invited_by'].unique()
    recruiter_ids = {name: i + 1 for i, name in enumerate(unique_recruiters)}

    # Replace the 'invited_by' column with 'recruiter_id'
    main_df['recruiter_id'] = main_df['invited_by'].map(recruiter_ids)
    main_df = main_df.reset_index(
        drop=True).reset_index(names=['applicant_id'])

    # Create a dataframe of unique recruiters and their IDs
    recruiters_df = pd.DataFrame(list(recruiter_ids.items()), columns=[
                                 'recruiter_name', 'recruiter_id'])

    return main_df, recruiters_df


def process_locations() -> pd.DataFrame:
    # print("Running recruit")
    main_df, recruiter_df = recruit()

    # Create a new dataframe 'location_df' with unique values of 'address', 'postcode', 'city', and 'applicant_id'
    # print("Creating address")
    #main_df = main_df.drop_duplicates().reset_index(drop=True).reset_index(names=['applicant_id'])

    personal_details_df = main_df[['applicant_id', 'name', 'gender', 'dob', 'email', 'phone_number', 'address', 'postcode', 'city','uni','degree']].drop_duplicates().reset_index(drop=True).reset_index(names=['person_id'])
    main_df = pd.merge(main_df, personal_details_df[['applicant_id','person_id']], on=['applicant_id'])

    address_df = personal_details_df[['address', 'postcode', 'city']].drop_duplicates().reset_index(drop=True).reset_index(names=['address_id'])
    # print("Merging main df")
    personal_details_df = pd.merge(personal_details_df, address_df, on=['address', 'postcode', 'city'])
    # print("Creating postcodes")
    postcode_df = address_df[['postcode']].drop_duplicates().reset_index(
        drop=True).reset_index(names=['postcode_id'])
    # print("Reeplacing postcodes")
    address_df.postcode = address_df.postcode.map(
        dict(zip(postcode_df.postcode.to_list(), postcode_df.postcode_id.to_list())))
    # print("Creating city")
    city_df = address_df[['city']].drop_duplicates().reset_index(
        drop=True).reset_index(names=['city_id'])
    # print("Replacing city")
    address_df.city = address_df.city.map(
        dict(zip(city_df.city.to_list(), city_df.city_id.to_list())))
    # print("Creating city")
    degree_df = personal_details_df[['degree']].drop_duplicates().reset_index(drop=True).reset_index(names=['degree_id'])
    # print("Replacing city")
    personal_details_df.degree = personal_details_df.degree.map(dict(zip(degree_df.degree.to_list(), degree_df.degree_id.to_list())))
    # print("Creating city")
    uni_df = personal_details_df[['uni']].drop_duplicates().reset_index(
        drop=True).reset_index(names=['uni_id'])
    # print("Replacing city")
    personal_details_df.uni = personal_details_df.uni.map(dict(zip(uni_df.uni.to_list(), uni_df.uni_id.to_list())))

    main_df = main_df.drop(
        ['address', 'postcode', 'city', 'invited_by'], axis=1)
    main_df = main_df.rename(
        columns={'address': 'address_id', 'uni': 'uni_id'})
    address_df = address_df.rename(
        columns={'postcode': 'postcode_id', 'city': 'city_id'})
    personal_details_df = personal_details_df.rename(
        columns={'degree': 'degree_id', 'uni': 'uni_id'})
    
    personal_details_df = personal_details_df.drop(['city','address','postcode','applicant_id'],axis=1)
    # Return all dataframes
    return main_df[['applicant_id','invited_date','person_id','recruiter_id']], personal_details_df, uni_df, degree_df, address_df, postcode_df, city_df, recruiter_df

# this is final method needed for sql, unless we need to write csv


def process_data_since(dt: datetime):

    main_df = getDataSince(dt)
    if not main_df.empty:
        main_df['invited_by'] = main_df['invited_by'].replace(
            'Bruno Belbrook', 'Bruno Bellbrook')
        main_df['invited_by'] = main_df['invited_by'].replace(
            'Fifi Etton', 'Fifi Eton')

        unique_recruiters = main_df['invited_by'].unique()
        recruiter_ids = {name: i + 1 for i,
                         name in enumerate(unique_recruiters)}

        # Replace the 'invited_by' column with 'recruiter_id'
        main_df['recruiter_id'] = main_df['invited_by'].map(recruiter_ids)

        # Create a dataframe of unique recruiters and their IDs
        recruiter_df = pd.DataFrame(list(recruiter_ids.items()), columns=[
                                    'recruiter_name', 'recruiter_id'])

        location_df = main_df[['address', 'postcode',
                               'city', 'applicant_id']].drop_duplicates()
        location_df['location_id'] = range(len(location_df))
        main_df.drop(['address', 'postcode', 'city',
                     'invited_by'], axis=1, inplace=True)
        # Return all dataframes
        return main_df, location_df, recruiter_df
    else:
        return "No New Records."

# def Update_Data(time):
#     dt=parse(time)
#     output= applicant_details_transformation_v2.process_data_since(dt)

#     if len(output) == 3:
#         main_df, location_df, recruiter_df = output
#         return main_df, location_df, recruiter_df
#     elif len(output) == 15:
#         print(output)
#     else:
#         print("Unexpected")
