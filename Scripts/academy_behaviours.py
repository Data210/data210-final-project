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

# CLEANING DATA
# Calculate the number of null values per column
null_values_count = df.isnull().sum()
# Calculate the percentage of null values per column
null_values_percentage = (null_values_count / len(df)) * 100
# Combine the counts and percentages into a single DataFrame
null_values_summary = pd.DataFrame({
    'Null Values Count': null_values_count,
    'Null Values Percentage': null_values_percentage
})

print(null_values_summary)
#see if we have duplicate names
duplicate_rows = df[df['name'].duplicated()]
duplicate_names = duplicate_rows['name'].unique()
print("Duplicate names:", list(duplicate_names))
# change in lower cases
# df['name'] = df['name'].str.lower()
# df['trainer'] = df['trainer'].str.lower()
# df['course_name'] = df['course_name'].str.lower()
#change columns to be in lower case
df.columns = [col.lower() for col in df.columns]

#to remove.0 from our values
cols_to_convert = [col for col in df.columns if col.startswith(('analytic_', 'independent_', 'determined_', 'professional_', 'studious_', 'imaginative_'))]
print(cols_to_convert)
df[cols_to_convert] = df[cols_to_convert].astype(str)
print(df[cols_to_convert])

df['str_date'] = df['date'].dt.strftime('%Y-%m-%d')
# print(df['str_date'])
df['behaviour_id'] = pd.concat([df['name'], df['str_date']], axis=1).apply(lambda x: ''.join(x), axis=1)
df['behaviour_id'] = df['behaviour_id'].str.replace(' ', '_')
#ely kely to elly kelly
df['trainer'] = df['trainer'].replace('Ely Kely', 'Elly Kelly')

#CREATING SPARTA TABLE
spartans = df[['behaviour_id','name', 'trainer', 'course_name']]
trainer_dict = {
    'Gregor Gomez': 1,
    'Bruce Lugo': 2,
    'Neil Mccarthy': 3,
    'Rachel Richard': 4,
    'Hamzah Melia': 5,
    'Burhan Milner': 6,
    'Elly Kelly': 7,
    'Trixie Orange': 8,
    'John Sandbox': 9,
    'Edward Reinhart': 10,
    'Lucy Foster': 11,
    'Gina Cartwright': 12,
    'Eshal Brandt': 13,
    'Macey Broughton': 14,
    'Igor Coates': 15,
    'Mohammad Velazquez': 16,
    'Martina Meadows': 17
}
spartans['trainer_id'] = spartans['trainer'].map(trainer_dict).astype(int)
spartans['course_id'] = spartans['course_name'].str.slice(stop=3).str.upper()



#CREATING BEHAVIOUR TABLE
#we want to separate weeks and traits
# Define columns to keep fixed
fixed_cols = ['behaviour_id', 'name', 'trainer', 'course_name', 'date']
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

behaviour = df_reshaped.drop(columns=['trainer', 'course_name', 'date'])
# removing the w in the week values
behaviour['week'] = behaviour['week'].str.replace('w', '')
# changing type to int in trait columns
behaviour[['analytic','determined','imaginative','independent','professional','studious']] = behaviour[['analytic','determined','imaginative','independent','professional','studious']].astype(int)

#CREATING COURSES TABLE
course = spartans[['course_name','course_id']]

#CREATING TRAINER TABLE
trainer = spartans[['trainer','trainer_id']]

#CREATING SPARTA TABLE
spartans = spartans[['behaviour_id', 'name', 'trainer_id', 'course_id']]



# print both tables
print(spartans.head(5))
print(behaviour.head(5))
print(course.head(5))
print(trainer.head(5))