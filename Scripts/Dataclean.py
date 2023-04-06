#imports
import boto3
import pandas as pd
import numpy as np
import string
pd.options.mode.chained_assignment = None  # default='warn'
#set up s3
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')
bucket_list = s3_client.list_buckets()
bucket_name = 'data-eng-210-final-project'
prefix = 'Talent'
bucket_contents= s3_client.list_objects_v2(Bucket=bucket_name)
bucket = s3_resource.Bucket(bucket_name)
objects = bucket.objects.all()
#get all keys
applicant_file_keys = []
for obj in objects.filter(Prefix=prefix):

     key=obj.key
     if key.endswith('.csv'):
         applicant_file_keys.append(key)
#create dataframe using first key
df = pd.read_csv(s3_client.get_object(Bucket = bucket_name, Key = applicant_file_keys[0])['Body'])
#combine all csvs into a dataframe using keys
for i in range (1, len(applicant_file_keys)):
    key_use = applicant_file_keys[i]
    test_key = s3_client.get_object(Bucket = bucket_name, Key = key_use)
    df_temp = pd.read_csv(test_key['Body'])
    df = pd.concat([df,df_temp],ignore_index=True)
#get null % per column
for column in df.columns:
    null_count=df['{}'.format(column)].isna().sum()
    null_per = 100*null_count/len(df['{}'.format(column)])
#     print(column,' ',null_per,'% of entries are null')
#create column for combined invited date
df['invited_date']=df['invited_date'].astype(str)
df['invited_date']=df['invited_date'].str.replace("\.0", "")
df['month'] = df['month'].str.upper()
df['Invited on'] = df['invited_date'] + ', '+ df['month']
#clean phone number column
phone_null=df['phone_number'].isna().sum()
df['phone_number']=df['phone_number'].astype(str)
df['phone_number']=df['phone_number'].str.replace("(", "")
df['phone_number']=df['phone_number'].str.replace(")", "")
df['phone_number']=df['phone_number'].str.replace(" ", "-")
#check for any problems
problems = 0
for phone in range(0,len(df['phone_number'])):
    if len(df['phone_number'][phone]) != 16:
        problems+=1
net_phone_problems=problems-phone_null
# print(net_phone_problems)
#create date for unique id
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
#handle nulls
df['iddate'] = df['iddate'].fillna('19700101')
df['iddate'] = df['iddate'].astype(str)
df['iddate']=df['iddate'].str.replace(".0", "")
#clean name for id
df['nameuse']=df['name'].str.replace(" ", "")
df['nameuse']=df['nameuse'].str.replace('[ '+string.punctuation+']','',regex=True)
df['nameuse']=df['nameuse'].str.lower()
#create unique id
df['uniqueid']=df['nameuse']+df['iddate']
#drop intermediate columns

df=df.drop(['month','year','month to use','nameuse'],axis=1)
problems = 0
for id in range(0,len(df['iddate'])):
    if len(df['iddate'][id]) ==7:
        word=df['iddate'][id]
        last_letter=df['iddate'][id][-1]
        word=word[:-1]
        df['iddate'][id]=word+'0'+last_letter