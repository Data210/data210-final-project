from s3 import S3Client
import pandas as pd
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
    Returns multiple DataFrames of normalized data
    """
    # Create empty df and loop through all files, parsing and concatenating to main df
    df = pd.DataFrame()
    df_tech_scores = pd.DataFrame()
    df_strengths = pd.DataFrame()
    df_weaknesses = pd.DataFrame()
    for key in keys:
        json_object = client.getJSON(bucket_name,key)
        temp_df, temp_df_tech_scores, temp_df_strengths, temp_df_weaknesses = parseFile(json_object)
        #Get unique ID from file name
        key_id = key.split('Talent/')[1].split('.json')[0]
        temp_df['json_key'] = key_id
        temp_df_tech_scores['json_key'] = key_id
        temp_df_strengths['json_key'] = key_id
        temp_df_weaknesses['json_key'] = key_id
        #temp_df_tech_scores = pd.melt(temp_df_tech_scores, id_vars='json_key', value_vars=temp_df_tech_scores.columns[:-1], var_name='language', value_name='score')
        
        #Add record to final DataFrames
        df = pd.concat([df,temp_df])
        df_tech_scores = pd.concat([df_tech_scores,temp_df_tech_scores])
        df_strengths = pd.concat([df_strengths,temp_df_strengths])
        df_weaknesses = pd.concat([df_weaknesses,temp_df_weaknesses])

    # Force correct types
    df_tech_scores['score'] = df_tech_scores['score'].astype(int)
    df['self_development'] = df['self_development'].map(dict(Yes=True,No=False))
    df['geo_flex'] = df['geo_flex'].map(dict(Yes=True,No=False))
    df['financial_support_self'] = df['financial_support_self'].map(dict(Yes=True,No=False))
    return df, df_tech_scores, df_strengths, df_weaknesses

# Save results to a file
def getAllDataAsCSV():
    """
    Writes ALL the related records in the bucket to a .csv
    """

    df, df_tech_scores, df_strengths, df_weaknesses = getAllData()

    with open('talent_details.csv', 'w') as file:
        df.to_csv(file)
    with open('tech_scores.csv', 'w') as file:
        df_tech_scores.to_csv(file)
    with open('strengths.csv', 'w') as file:
        df_strengths.to_csv(file)
    with open('weaknesses.csv', 'w') as file:
        df_weaknesses.to_csv(file)

# Utility function for parsing the file
def parseFile(json_object: str) -> pd.DataFrame:
    """
    Takes in a JSON object from the Talent .json files and returns multiple DataFrames of normalized data
    """
    languages = json_object.get('tech_self_score',{}).keys()
    scores = json_object.get('tech_self_score',{}).values()
    df_tech_scores = pd.DataFrame({'language': languages, 'score':scores})
    df_strengths = pd.DataFrame({'strengths':json_object.get('strengths',[])})
    df_weaknesses = pd.DataFrame({'weaknesses':json_object.get('weaknesses',[])})
    #df_tech_scores = pd.DataFrame.from_dict([json_object.get('tech_self_score',{})])
    df = pd.DataFrame({key: json_object[key] for key in json_object if key not in ['tech_self_score','strengths','weaknesses']},index=[0])
    return df, df_tech_scores, df_strengths, df_weaknesses
