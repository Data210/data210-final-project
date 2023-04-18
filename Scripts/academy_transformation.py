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
    for item in client.getAllObjects(bucket_name).filter(Prefix='Academy'):
        if item.key.endswith('.csv'):
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

    for key in keys:
        file_df = client.getDataFrame(bucket_name,key)
        temp_df = parseFile(file_df)
        #Get unique ID from file name
        key = key.split('/')[-1]
        key = key.rsplit('.',1)[0]
        course_name, course_num, date = key.split('_', 2)
        temp_df['course_name'] = course_name
        temp_df['course_num'] = course_num
        temp_df['date'] = date
        
        #Add record to final DataFrames
        df = pd.concat([df,temp_df])

    df.columns = [col.lower() for col in df.columns]
    behaviour_cols = [col for col in df.columns if col.startswith(('analytic_', 'independent_', 'determined_', 'professional_', 'studious_', 'imaginative_'))]
    #df[cols_to_convert] = df[cols_to_convert].astype(str)

    #ely kely to elly kelly
    df['trainer'] = df['trainer'].replace('Ely Kely', 'Elly Kelly')

    df = df.drop_duplicates().reset_index(drop=True).reset_index(names=['spartan_id'])
    df_course = df[['course_name','course_num','date']].drop_duplicates().reset_index(drop=True).reset_index(names=['course_id'])
    df = pd.merge(df,df_course).drop(columns=['course_name','course_num'])

    df_trainer = df[['trainer']].drop_duplicates().reset_index(drop=True).reset_index(names=['trainer_id'])
    df.trainer = df.trainer.map(dict(zip(df_trainer.trainer.to_list(), df_trainer.trainer_id.to_list())))

    df_stream = df_course[['course_name']].drop_duplicates().reset_index(drop=True).reset_index(names=['stream_id'])
    df_course.course_name = df_course.course_name.map(dict(zip(df_stream.course_name.to_list(),df_stream.stream_id.to_list())))

    df = df.rename(columns={'trainer':'trainer_id'})
    df_course = df_course.rename(columns={'course_name':'stream_id'})
    df_stream = df_stream.rename(columns={'course_name':'stream'})

    df_behaviour_score = df[['spartan_id'] + behaviour_cols].melt(id_vars=['spartan_id'], value_vars=behaviour_cols, var_name='trait_week',value_name='score')
    df_behaviour_score = df_behaviour_score.dropna(how='any')
    df_behaviour_score[['behaviour', 'week']] = df_behaviour_score['trait_week'].str.split('_', expand=True)
    df_behaviour_score = df_behaviour_score.drop('trait_week', axis=1)
    #df_behaviour_score.behaviour = df_behaviour_score.behaviour.str.title()
    df_behaviour_score.week = df_behaviour_score.week.apply(lambda x: x[1:]).astype(int)
    df_behaviour_score.score = df_behaviour_score.score.astype(int)
    df_behaviour_score = df_behaviour_score.pivot(index= ['spartan_id','week'], columns=['behaviour'], values=['score'])
    df_behaviour_score.columns = df_behaviour_score.columns.droplevel(0)
    df_behaviour_score = df_behaviour_score.reset_index()
    #df_behaviour_score = df_behaviour_score.reset_index(drop=True).reset_index(names=['behaviour_score_id'])

    # df_behaviour = df_behaviour_score[['behaviour']].drop_duplicates().reset_index(drop=True).reset_index(names=['behaviour_id'])
    # df_behaviour_score.behaviour = df_behaviour_score.behaviour.map(dict(zip(df_behaviour.behaviour.to_list(),df_behaviour.behaviour_id.to_list())))
    

    df_course = df_course.rename(columns={'date':'start_date'})
    df_course.start_date = pd.to_datetime(df_course.start_date,dayfirst=False)

    return df[['spartan_id','course_id','trainer_id','name']], df_course, df_stream, df_trainer, df_behaviour_score #, df_behaviour

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
def parseFile(file_df: pd.DataFrame) -> pd.DataFrame:
    """
    Simply returns the DataFrame as no parsing is needed
    """
    return file_df
