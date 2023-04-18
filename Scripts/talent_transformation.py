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
    df_tech_junction = pd.DataFrame()
    df_strength_junction = pd.DataFrame()
    df_weakness_junction = pd.DataFrame()
    for key in keys:
        json_object = client.getJSON(bucket_name,key)
        temp_df, temp_df_strength, temp_df_weakness, temp_df_tech_score = parseFile(json_object)
        #Get unique ID from file name
        key_id = key.split('Talent/')[1].split('.json')[0]
        temp_df['talent_id'] = key_id
        temp_df_tech_score['talent_id'] = key_id
        temp_df_strength['talent_id'] = key_id
        temp_df_weakness['talent_id'] = key_id
        #temp_df_tech_score = pd.melt(temp_df_tech_score, id_vars='json_key', value_vars=temp_df_tech_score.columns[:-1], var_name='language', value_name='score')
        
        #Add record to final DataFrames
        df = pd.concat([df,temp_df])
        df_tech_junction = pd.concat([df_tech_junction,temp_df_tech_score])
        df_strength_junction = pd.concat([df_strength_junction,temp_df_strength])
        df_weakness_junction = pd.concat([df_weakness_junction,temp_df_weakness])

    # Force correct types
    df_tech_junction['score'] = df_tech_junction['score'].astype(int)
    df['date'] = pd.to_datetime(df.date.str.replace('//','/'),dayfirst=True)
    df['self_development'] = df['self_development'].map(dict(Yes=True,No=False))
    df['geo_flex'] = df['geo_flex'].map(dict(Yes=True,No=False))
    df['financial_support_self'] = df['financial_support_self'].map(dict(Yes=True,No=False))
    df['result'] = df['result'].map({'Pass': True, 'Fail':False})

    df_strength = df_strength_junction.strength.to_frame().drop_duplicates().reset_index(drop=True).reset_index(names=['strength_id'])
    df_weakness = df_weakness_junction.weakness.to_frame().drop_duplicates().reset_index(drop=True).reset_index(names=['weakness_id'])
    df_tech = df_tech_junction.language.to_frame().drop_duplicates().reset_index(drop=True).reset_index(names=['tech_id'])

    df_strength_junction.strength = df_strength_junction.strength.map(dict(zip(df_strength.strength.to_list(),df_strength.strength_id.to_list())))
    df_weakness_junction.weakness = df_weakness_junction.weakness.map(dict(zip(df_weakness.weakness.to_list(),df_weakness.weakness_id.to_list())))
    df_tech_junction.language = df_tech_junction.language.map(dict(zip(df_tech.language.to_list(),df_tech.tech_id.to_list())))

    df_strength_junction = df_strength_junction.rename(columns={'strength':'strength_id'})
    df_weakness_junction = df_weakness_junction.rename(columns={'weakness':'weakness_id'})
    df_tech_junction = df_tech_junction.rename(columns={'language':'tech_id'})

    df_tech = df_tech.rename(columns={'language':'tech_name','id':'tech_id'})
    df = df.rename(columns={'result':'pass','course_interest':'stream_id'})

    return df, df_strength_junction, df_weakness_junction, df_tech_junction, df_strength, df_weakness, df_tech,

# Save results to a file
def getAllDataAsCSV():
    """
    Writes ALL the related records in the bucket to a .csv
    """

    df, df_strengths, df_weaknesses, df_tech_scores  = getAllData()

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
    df_strengths = pd.DataFrame({'strength':json_object.get('strengths',[])})
    df_weaknesses = pd.DataFrame({'weakness':json_object.get('weaknesses',[])})
    #df_tech_scores = pd.DataFrame.from_dict([json_object.get('tech_self_score',{})])
    df = pd.DataFrame({key: json_object[key] for key in json_object if key not in ['tech_self_score','strengths','weaknesses']},index=[0])
    return df, df_strengths, df_weaknesses, df_tech_scores
