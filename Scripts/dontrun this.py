from s3 import S3Client
import pandas as pd
import io
import re


#Config
bucket_name = "data-eng-210-final-project"
client = S3Client()

#Find all files with .txt extension in bucket
sparta_day_file_keys = []
for item in client.getAllObjects(bucket_name).filter(Prefix='Talent'):
    if item.key.endswith('.txt'):
        sparta_day_file_keys.append(item.key)


# Utility function for parsing the sparta day .txt format
def parseTextFile(text: str) -> list:
    """
    Takes in a string from the Sparta Day .txt files' body and returns a list of rows, each row is a list split by column
    """
    split = text.replace('\r','').split('\n')
    table = []
    for line in split[3:-1]:
        try:
            names, rest = line.rsplit("-",1)
        except:
            print(line)
        names = names.replace(',','')
        columns = (names +','+ rest).split(',')
        row = columns[0].split(" ",1)+[re.search('([0-9]{1,3})/', columns[-2]).group(1), re.search('([0-9]{1,3})/', columns[-1]).group(1)]+[split[0], split[1].split(' ')[0]]
        table.append(row)
    return table
import webbrowser as wb


# Create empty df and loop through all files, parsing and concatenating to main df
sparta_day_df = pd.DataFrame()
for key in sparta_day_file_keys:
    text = client.getCSV(bucket_name,key)
    sparta_day_df = pd.concat([sparta_day_df,pd.DataFrame(parseTextFile(text))])

# Set column names
sparta_day_df.columns = ["FirstName","LastName","Psychometrics","Presentation","Date","Academy"]
wb.open('https://www.youtube.com/watch?v=dQw4w9WgXcQ')
#Additional cleaning, strip whitespace, enforce data types
sparta_day_df.FirstName = sparta_day_df.FirstName.str.strip(" ")
sparta_day_df.LastName = sparta_day_df.LastName.str.strip(" ")
sparta_day_df.Psychometrics = sparta_day_df.Psychometrics.astype(int)
sparta_day_df.Presentation = sparta_day_df.Presentation.astype(int)
sparta_day_df[sparta_day_df.LastName.str.contains(" ")]

#Save results to a file for now
with open('sparta_day_clean.csv','w') as file:
    sparta_day_df.to_csv(file)



