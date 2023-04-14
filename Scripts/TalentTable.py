import boto3
import json
import pandas as pd
import io
import sparta_day_transformation
import string

def Talent():

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

    df = sparta_day_transformation.getAllData()

    df_table = df_all[['json_key','name','date','self_development','financial_support_self','result','course_interest','geo_flex']]

    df_table['date'] = df_table['date'].str.replace('//', '/')
    df_table['date'] = pd.to_datetime(df_table['date'], format='%d/%m/%Y')

    # convert the datetime values to a string with the desired format
    df_table['date'] = df_table['date'].dt.strftime('%Y%m%d')


    df_table['namestring'] = df_table['name'].str.replace('[ '+string.punctuation+']', '', regex=True)

    df_table['namestring'] = df_table['namestring'].str.lower()

    df_table['SpartaDayTalentID'] = df_table['namestring'].str.cat(df_table['date'], sep='')

    df_table = df_table.drop_duplicates()

    df_t = pd.merge(df_table, df, on='SpartaDayTalentID', how='outer')

    df_clean = df_t[['json_key','SpartaDayTalentID','name','Date','self_development','financial_support_self','result','course_interest','Psychometrics','Presentation','geo_flex','Academy']]

    df_clean['academyid'] = pd.factorize(df_clean['Academy'])[0]
    df_clean['academyid'] = df_clean['academyid'].astype('int')
    df_clean['academyid'] = df_clean['academyid'] + 1
    academy_table = df_clean[['academyid', 'Academy']].drop_duplicates().reset_index(drop=True)
    df_clean = df_clean.drop(columns=['Academy'])

    df_clean.rename(columns={"SpartaDayTalentID": "applicant_id", "academyid": "academy_location_id", "Psychometrics": "psychometric_result", "Presentation": "presentation_result"}, inplace=True)
    academy_table.rename(columns={"academyid": "academy_location_id", "Academy": "location_name"}, inplace=True)

    Talent = df_clean
    Talent.reset_index(drop=True, inplace=True)


    Academy_Locations = academy_table
    Academy_Locations.reset_index(drop=True, inplace=True)

    return Talent, Academy_Locations
