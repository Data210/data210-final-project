import boto3
import json
import pandas as pd
import io
import string

def technologies():

    s3 = boto3.resource('s3')
    bucket_name = 'data-eng-210-final-project'
    bucket = s3.Bucket('data-eng-210-final-project')

    # Initalize DataFrame
    df_all=pd.DataFrame()

    # Put all jsons into a single DataFrame with the json key
    for obj in bucket.objects.all():
        if obj.key.endswith('.json'):
            x = obj.key
            obj = s3.Object(bucket_name, x)
            json_data = obj.get()['Body'].read().decode('utf-8')
            data = json.loads(json_data)
            df = pd.json_normalize(data)
            
            text = x
            result = text.split('Talent/')[1].split('.json')[0]
            df['json_key'] = result
            
            df_all = pd.concat([df_all, df])

    import sparta_day_transformation
    df = sparta_day_transformation.getAllData()

    df_all['date'] = df_all['date'].str.replace('//', '/')
    df_all['date'] = pd.to_datetime(df_all['date'], format='%d/%m/%Y')

    # convert the datetime values to a string with the desired format
    df_all['date'] = df_all['date'].dt.strftime('%Y%m%d')


    df_all['namestring'] = df_all['name'].str.replace('[ '+string.punctuation+']', '', regex=True)
    df_all['namestring'] = df_all['namestring'].str.lower()

    df_all['SpartaDayTalentID'] = df_all['namestring'].str.cat(df_all['date'], sep='')

    df_t = pd.merge(df_all, df, on='SpartaDayTalentID', how='outer')

    filtered_df = df_t.filter(regex='^tech_self_score|^SpartaDayTalentID$')
    filtered_df.columns = filtered_df.columns.str.replace("tech_self_score.", "")

    df = filtered_df.melt(id_vars='SpartaDayTalentID', value_vars=filtered_df.columns[:-1], var_name='languages', value_name='scores')
    df = df.dropna()
    df = df.sort_values(by='SpartaDayTalentID')
    df['language_id'] = pd.factorize(df['languages'])[0]
    df=df.reset_index(drop=True)

    language_table = df[['language_id', 'languages']].drop_duplicates().reset_index(drop=True)
    tech_self_scores = df[['language_id','SpartaDayTalentID','scores']]
    tech_self_scores.rename(columns={"language_id": "tech_id", "SpartaDayTalentID": "applicant_id", "scores": "score"}, inplace=True)
    language_table.rename(columns={"languages": "techname", "language_id": "tech_id"}, inplace=True)

    technologies = language_table
    technologies = technologies.reset_index(drop=True)

    tech_junction = tech_self_scores
    tech_junction = tech_junction.reset_index(drop=True)

    return technologies, tech_junction

