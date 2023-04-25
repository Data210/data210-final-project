from s3 import S3Client
import pandas as pd
import re
import string
import io
from datetime import datetime, timezone
from dateutil.parser import parse
from create_database import create_database
from sqlalchemy import text

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
    csvs,pool_keys = client.getObjectsPooled(keys, bucket_name, 'csv')
    for key, file_csv in zip(pool_keys, csvs):
        #text = client.getCSV(bucket_name, key)
        df = pd.concat([df, pd.DataFrame(parseFile(file_csv))])
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
    # return getData(applicants_file_keys)
    return getData(applicants_file_keys)


def getAllDataAsCSV(filename='applicants_clean.csv'):
    """
    Writes ALL the related records in the bucket to a .csv
    """
    with open(filename, 'w') as file:
        getAllData().to_csv(file)


def process_locations(engine) -> pd.DataFrame:
    # print("Running recruit")
    # main_df, recruiter_df = recruit()
    main_df = getAllData()
    main_df.name = main_df.name.map(lambda x: x.title())

    with engine.connect() as conn:
        current_main_df = pd.read_sql(text("""SELECT app.applicant_id, pd.person_id, pd.name, pd.gender, pd.dob, 
        pd.email, pd.phone_number, ad.address_id, ad.address, pos.postcode_id, pos.postcode, c.city_id, c.city, u.uni_id, 
        u.uni, d.degree_id, d.degree, rec.recruiter_id, rec.recruiter_name FROM Applicant as app
                                      JOIN Personal_Details as pd ON pd.person_id = app.person_id
                                      JOIN Recruiter as r ON r.recruiter_id = app.recruiter_id
                                      JOIN University as u ON u.uni_id = pd.uni_id
                                      JOIN Degree as d ON d.degree_id = pd.degree_id
                                      JOIN Address as ad ON ad.address_id = pd.address_id
                                      JOIN Postcode as pos ON pos.postcode_id = ad.postcode_id
                                      JOIN City as c ON c.city_id = ad.city_id
                                      JOIN Recruiter rec ON rec.recruiter_id = app.recruiter_id"""), conn)
    
    main_df['city'] = main_df['city'].replace('Weston','Burnley')
    '''======================================================================================'''
    main_df = main_df.drop_duplicates()
    current_main_df['dob'] = pd.to_datetime(current_main_df['dob'])

    main_df = main_df\
        .merge(current_main_df.drop(['applicant_id','person_id','address_id','postcode_id','city_id','uni_id','degree_id','recruiter_id','recruiter_name'],axis=1), how="left", indicator=True)\
        .query("_merge == 'left_only'").drop('_merge',axis=1).reset_index(drop=True)

    main_df['applicant_id'] = pd.RangeIndex(len(main_df)) + (current_main_df['applicant_id'].max() if not pd.isna(float(current_main_df['applicant_id'].max())) else -1) + 1

    '''======================================================================================'''

    main_df['invited_by'] = main_df['invited_by'].replace(
        'Bruno Belbrook', 'Bruno Bellbrook')
    main_df['invited_by'] = main_df['invited_by'].replace(
        'Fifi Etton', 'Fifi Eton')
    main_df.rename(columns = {'invited_by':'recruiter_name'}, inplace=True)

    current_recruiter_df = current_main_df[['recruiter_id','recruiter_name']]
    recruiter_df = main_df[['recruiter_name']].drop_duplicates()

    recruiter_df = recruiter_df\
        .merge(current_recruiter_df.drop(['recruiter_id'],axis=1), how="left", indicator=True)\
        .query("_merge == 'left_only'").drop('_merge',axis=1).reset_index(drop=True)

    recruiter_df['recruiter_id'] = pd.RangeIndex(len(recruiter_df)) + (current_recruiter_df['recruiter_id'].max() if not pd.isna(float(current_recruiter_df['recruiter_id'].max())) else -1) + 1

    main_df.recruiter_name = main_df.recruiter_name.map(
        dict(pd.concat([recruiter_df, current_recruiter_df])[['recruiter_name','recruiter_id']].values.tolist()))

    '''======================================================================================'''

    current_details_df = current_main_df[['applicant_id', 'person_id','name','gender','dob','email','phone_number','address','postcode','city','uni','degree']]
    personal_details_df = main_df[['applicant_id','name','gender','dob','email','phone_number','address','postcode','city','uni','degree']]
    personal_details_df = personal_details_df\
        .merge(current_details_df.drop(['applicant_id'],axis=1), how="left", indicator=True)\
        .query("_merge == 'left_only'").drop('_merge',axis=1).reset_index(drop=True)
    
    personal_details_df['person_id'] = pd.RangeIndex(len(personal_details_df)) + (current_details_df['person_id'].max() if not pd.isna(float(current_details_df['person_id'].max())) else -1) + 1


    main_df = pd.merge(main_df, personal_details_df[['applicant_id','person_id']], on=['applicant_id'])


    '''======================================================================================'''

    current_address_df = current_main_df[['address_id','address','postcode','city']]
    # Creating addresses
    address_df = personal_details_df[['address', 'postcode', 'city']].drop_duplicates()

    address_df = address_df\
        .merge(current_address_df.drop(['address_id'],axis=1), how="left", indicator=True)\
        .query("_merge == 'left_only'").drop('_merge',axis=1).reset_index(drop=True)

    address_df['address_id'] = pd.RangeIndex(len(address_df)) + (current_address_df['address_id'].max() if not pd.isna(float(current_address_df['address_id'].max())) else -1) + 1


    # print("Merging main df")
    personal_details_df = pd.merge(personal_details_df, address_df, on=['address', 'postcode', 'city'])


    '''======================================================================================'''

    current_postcode_df = current_main_df[['postcode_id','postcode']]
    postcode_df = main_df[['postcode']].drop_duplicates()
    # print("Creating postcodes")

    postcode_df = postcode_df\
        .merge(current_postcode_df.drop(['postcode_id'],axis=1), how="left", indicator=True)\
        .query("_merge == 'left_only'").drop('_merge',axis=1).reset_index(drop=True)

    postcode_df['postcode_id'] = pd.RangeIndex(len(postcode_df)) + (current_postcode_df['postcode_id'].max() if not pd.isna(float(current_postcode_df['postcode_id'].max())) else -1) + 1


    # print("Reeplacing postcodes")
    address_df.postcode = address_df.postcode.map(
        dict(pd.concat([postcode_df, current_postcode_df])[['postcode','postcode_id']].values.tolist()))   
    
    '''======================================================================================'''
    
    current_city_df = current_main_df[['city_id','city']]
    city_df = main_df[['city']].drop_duplicates()
    # print("Creating city")

    city_df = city_df\
        .merge(current_city_df.drop(['city_id'],axis=1), how="left", indicator=True)\
        .query("_merge == 'left_only'").drop('_merge',axis=1).reset_index(drop=True)

    city_df['city_id'] = pd.RangeIndex(len(city_df)) + (current_city_df['city_id'].max() if not pd.isna(float(current_city_df['city_id'].max())) else -1) + 1


    # print("Replacing city")
    address_df.city = address_df.city.map(
        dict(pd.concat([city_df, current_city_df])[['city','city_id']].values.tolist()))

    '''======================================================================================'''

    current_degree_df = current_main_df[['degree_id','degree']]
    degree_df = main_df[['degree']].drop_duplicates()
    # print("Creating degree")

    degree_df = degree_df\
        .merge(current_degree_df.drop(['degree_id'],axis=1), how="left", indicator=True)\
        .query("_merge == 'left_only'").drop('_merge',axis=1).reset_index(drop=True)
    # print("Replacing degree")

    degree_df['degree_id'] = pd.RangeIndex(len(degree_df)) + (current_degree_df['degree_id'].max() if not pd.isna(float(current_degree_df['degree_id'].max())) else -1) + 1

    personal_details_df.degree = personal_details_df.degree.map(dict(pd.concat([degree_df, current_degree_df])[['degree','degree_id']].values.tolist()))

    '''======================================================================================'''

    current_uni_df = current_main_df[['uni_id','uni']]
    uni_df = main_df[['uni']].drop_duplicates()
    # print("Creating uni")

    uni_df = uni_df\
        .merge(current_uni_df.drop(['uni_id'],axis=1), how="left", indicator=True)\
        .query("_merge == 'left_only'").drop('_merge',axis=1).reset_index(drop=True)
    # print("Replacing uni")
    uni_df['uni_id'] = pd.RangeIndex(len(uni_df)) + (current_uni_df['uni_id'].max() if not pd.isna(float(current_uni_df['uni_id'].max())) else -1) + 1

    personal_details_df.uni = personal_details_df.uni.map(dict(pd.concat([uni_df, current_uni_df])[['uni','uni_id']].values.tolist()))

    '''======================================================================================'''

    main_df = main_df.rename(
        columns={'recruiter_name':'recruiter_id'})
    personal_details_df = personal_details_df.rename(
        columns={'uni':'uni_id','degree':'degree_id'}
    )
    address_df = address_df.rename(
        columns={'postcode':'postcode_id','city':'city_id'}
    )

    main_df = main_df[['applicant_id','invited_date','person_id','recruiter_id']]
    personal_details_df = personal_details_df[['person_id','name','gender','dob','email','phone_number','address_id','degree_id','uni_id']]
    
    # Return all dataframes
    return main_df, personal_details_df, uni_df, degree_df, address_df, postcode_df, city_df, recruiter_df

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

if __name__ == '__main__':
    engine = create_database()
    # all_keys = getAllData()
    # data = getData([all_keys[0]])
    process_locations(engine)
    # print(data)