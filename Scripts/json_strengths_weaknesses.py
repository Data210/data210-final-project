import pandas as pd
from s3 import S3Client
import string


def pull_json_from_s3(bucket_name, prefix):
    """
    Pulls JSON data from an S3 bucket and converts it to a pandas DataFrame.

    Args:
    - bucket_name: name of the bucket containing applicant json files
    - prefix: prefix directory filter containing applicant json files

    Returns:
    - A pandas DataFrame containing all of the applicant JSON data from the S3 bucket.
    """
    # Set up S3 client
    client = S3Client()

    # Initialize empty list to store DataFrames
    json_df = []

    try:
        # Iterate over all JSON files in S3 bucket with given prefix
        response = client.getAllObjects(bucket_name).filter(Prefix=prefix)

        for obj in response:
            key = obj.key
            if key.endswith('.json'):
                data = client.getJSON(bucket_name, key)
                try:
                    df = pd.json_normalize(data)
                    df['json_key'] = key.split('Talent/')[1].split('.json')[0]
                except ValueError:
                    print(f"Unable to normalize data from {key}")
                    continue
                # Append DataFrame to list
                json_df.append(df)
    except Exception as e:
        print(f"Error in retrieving data from S3: {str(e)}")
        return None

    # Check if any JSON files were retrieved
    if len(json_df) == 0:
        print(f"No JSON files found in S3 bucket: {bucket_name} with prefix: {prefix}")
        return pd.DataFrame()

    # Concatenate all DataFrames into a single DataFrame
    df = pd.concat(json_df, ignore_index=True)

    # Replace double slashes with single slashes in the 'date' column
    df['date'] = df['date'].str.replace('//', '/')

    # Add sparta_day_person_id
    df["applicant_id"] = df["name"].str.lower() + pd.to_datetime(df['date'], dayfirst=True).dt.strftime('%Y%m%d')
    df["applicant_id"] = df["applicant_id"].str.replace('[ ' + string.punctuation + ']', '', regex=True)

    df = df.set_index('applicant_id')
    df = df.reset_index()

    return df


def generate_strengths(df):
    """
    Generates a list of unique strengths and a junction table linking applicants to their strengths.

    Args:
    - df: a pandas DataFrame containing applicant data

    Returns:
    - A pandas DataFrame containing unique strengths and their IDs.
    - A pandas DataFrame linking applicants to their strengths.
    """
    # use explode function to split the list column into multiple rows
    if 'strengths' not in df.columns:
        print("Column 'strengths' not found in input DataFrame")
        return None, None

    try:
        df_strengths = df[['json_key', 'strengths']].explode('strengths')
        df_strengths = df_strengths.dropna()
        df_strengths = df_strengths.set_index('json_key')
        df_strengths = df_strengths.reset_index()

        unique_strengths = df_strengths['strengths'].unique()
        # Create a new dataframe with strength_id and strength columns
        strengths = pd.DataFrame({'strength_id': range(len(unique_strengths)),
                                  'strength': unique_strengths})

        df_strengths_joined = pd.merge(df_strengths, strengths, left_on='strengths', right_on='strength',
                                       how='left')
        strengths_junction = df_strengths_joined[['json_key', 'strength_id']]

        return strengths, strengths_junction
    except Exception as e:
        print(f"Error in generating strengths data: {str(e)}")
        return None, None


def generate_weaknesses(df):
    """
   Generate weaknesses and weaknesses_junction dataframes from the given input dataframe.

   Args:
       df: Input dataframe containing applicant_id and weaknesses columns.

   Returns:
       weaknesses: Dataframe containing unique weaknesses and their corresponding weakness_id.
       weaknesses_junction: Dataframe containing applicant_id and their corresponding weakness_id.
   """
    # use explode function to split the list column into multiple rows
    if 'weaknesses' not in df.columns:
        print("Column 'weaknesses' not found in input DataFrame")
        return None, None

    try:
        # use explode function to split the list column into multiple rows
        df_weaknesses = df[['json_key', 'weaknesses']].explode('weaknesses')
        df_weaknesses = df_weaknesses.dropna()
        df_weaknesses = df_weaknesses.set_index('json_key')
        df_weaknesses = df_weaknesses.reset_index()
        #print(df_weaknesses.head())

        unique_weaknesses = df_weaknesses['weaknesses'].unique()
        # Create a new dataframe with strength_id and strength columns
        weaknesses = pd.DataFrame({'weakness_id': range(len(unique_weaknesses)),
                                   'weakness': unique_weaknesses})

        df_weaknesses_joined = pd.merge(df_weaknesses, weaknesses, left_on='weaknesses', right_on='weakness',
                                        how='left')
        weaknesses_junction = df_weaknesses_joined[['json_key', 'weakness_id']]

        return weaknesses, weaknesses_junction
    except Exception as e:
        print(f"Error in generating weaknesses data: {str(e)}")
        return None, None


if __name__ == '__main__':

    df = pull_json_from_s3('data-eng-210-final-project', "Talent")
    print(df.head())
    with open('output/df_json.csv', 'w') as file:
        df.to_csv(file)

    strengths, strengths_junction = generate_strengths(df)
    print(strengths.head())
    with open('output/strengths.csv', 'w') as file:
        strengths.to_csv(file)
    print(strengths_junction.head())
    with open('output/strengths_junction.csv', 'w') as file:
        strengths_junction.to_csv(file)

    weaknesses, weaknesses_junction = generate_weaknesses(df)
    print(weaknesses.head())
    with open('output/weaknesses.csv', 'w') as file:
        weaknesses.to_csv(file)
    print(weaknesses_junction.head())
    with open('output/weaknesses_junction.csv', 'w') as file:
        weaknesses_junction.to_csv(file)




