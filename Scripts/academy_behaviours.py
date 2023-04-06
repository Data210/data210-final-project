import boto3
import io
import pandas as pd
import re
s3 = boto3.resource('s3')
bucket_name = 'data-eng-210-final-project'
prefix = 'Academy'

bucket = s3.Bucket(bucket_name)
data = []

for obj in bucket.objects.filter(Prefix=prefix):
 if obj.key.endswith('.csv'):
    key = obj.key.replace('.csv','')
    course_name = key.rsplit('_', 2)[0].split('/')[1]
    date = key.rsplit('_', 2)[-1]
    body = obj.get()['Body'].read().decode('utf-8')
    df = pd.read_csv(io.StringIO(body))
    df['course_name'] = course_name
    df['date'] = pd.to_datetime(date)
    data.append(df)

# Concatenate all data into one DataFrame
df = pd.concat(data, ignore_index=True)
df
print(df.info())

# Calculate the number of null values per column
null_values_count = df.isnull().sum()

# Calculate the percentage of null values per column
null_values_percentage = (null_values_count / len(df)) * 100

# Combine the counts and percentages into a single DataFrame
null_values_summary = pd.DataFrame({
    'Null Values Count': null_values_count,
    'Null Values Percentage': null_values_percentage
})

# Display the summary DataFrame
print(null_values_summary)

#see if we have duplicate names
duplicate_rows = df[df['name'].duplicated()]
duplicate_names = duplicate_rows['name'].unique()

print("Duplicate names:", list(duplicate_names))

# change name and trainer in lower cases
df['name'] = df['name'].str.lower()
df['trainer'] = df['trainer'].str.lower()

#change columns to be in lower case
df.columns = [col.lower() for col in df.columns]

#to remove.0 from our values
cols_to_convert = [col for col in df.columns if col.startswith(('analytic_', 'independent_', 'determined_', 'professional_', 'studious_', 'imaginative_'))]

df[cols_to_convert] = df[cols_to_convert].astype(str).apply(lambda x: x.str.split('.').str[0])

#!!! Creating the academy table
Academy = df[['name', 'trainer', 'course_name', 'date']]
print(Academy)


#we want to separate weeks and traits
# Define columns to keep fixed

fixed_cols = ['name', 'trainer', 'course_name', 'date']

# Define columns to unpivot
unpivot_cols = [col for col in df.columns if col.startswith(('analytic_', 'independent_', 'determined_', 'professional_', 'studious_', 'imaginative_'))]
df[unpivot_cols] = df[unpivot_cols].astype(str)

# Unpivot columns
df_unpivoted = df.melt(id_vars=fixed_cols, value_vars=unpivot_cols, var_name='trait_week')

# Split 'trait_week' column into 'trait' and 'week' columns
df_unpivoted[['index', 'week']] = df_unpivoted['trait_week'].str.split('_', expand=True)

# Drop 'trait_week' column
df_unpivoted.drop('trait_week', axis=1, inplace=True)

# Pivot the dataframe to reshape it
df_reshaped = df_unpivoted.pivot_table(index=fixed_cols + ['week'], columns='index', values='value').reset_index()

#!!! Creating Behaviour table
Behaviour = df_reshaped.drop(columns=['trainer', 'course_name', 'date'])

#reshaping made our values as float again, we convert it to int. 
Behaviour[['analytic', 'determined', 'imaginative', 'independent', 'professional', 'studious']] = Behaviour[['analytic', 'determined', 'imaginative', 'independent', 'professional', 'studious']].applymap(lambda x: int(str(x).replace('.0', '')) if isinstance(x, float) else x)
Behaviour.dtypes