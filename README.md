# data210-final-project

### Requirements:
  - Docker container 
  - Microsoft SQL server hosted locally
  - Python modules as per requirements.txt

# Set-up Steps:
### Create Database
  - Set up your own Database, can either be set up locally or hosted remotely, as long as you have a connection string.
### Set up connection info
  - Add your connection string information to the config.ini file.
  - Add your `DB_USERNAME` and `DB_PASSWORD`, to your .env file.
### Run main.py
  - After setting up the connection, running main.py should add all the data to your database.


# Documentation on /Scripts:

### main.py
Point of entry for the pipeline, this creates the tables, extracts and transforms the data in S3 and inserts into the databasee

#### s3.py
Complies commonly used functions to be callable in other scripts.

#### applicant_details_transformation.py
Creates dataframe from all applicant csv, cleans data and creates unique ID.

#### academy_cleaned.ipynb
Creates dataframe from all academy csv, cleans data. (Note:will be changed to .py in future)

#### connection_string.py
Creates a connection string from the contents of the config_file.ini in Scrpts folder and .env file in the data210-final-project folder.

config_file.ini contains info on the database, .env file contains username and password info.

Connection string has structure : {dialect}://{db_username}:{db_password}@{server}:{port}/{database_name}?driver={driver}

#### json_pandas.py
Converts data provided by client in JSON format to a pandas dataframe, then creates seperate dataframes for strenghts and weaknesses.

#### sparta_day_transformation.py
Defines a function to parse text files provided by client, reads text files and writes them to a pandas dataframe, cleans the data and creates a unqiue ID. 

#### create_database.py
Creates initial database and tables using SQLAlchemy.

#### academy_behaviours.py
Creates behaviour, spartans, course and trainer dataframes from academy data.

#### self_tech_scores.py
Obtains the tech scores from JSON files as a csv.

#### talent_sparta_results.py
Outputs key interview metrics as a csv.

# Original Entity Relationship Diagram

![alt text](https://github.com/Data210/data210-final-project/blob/bcde497a5d88bd809fb81ca5c724d9f70e3f3be8/Viz/original_erd.png?raw=true)

# Tableau Cloud Output 

#### Applicant Overview Dashboard 

![alt text](https://github.com/Data210/data210-final-project/blob/fe579e507d969f2cf8b52c38f5c0a79d2e56a499/Viz/Sparta_Applicant_Dashbaord_Overview.png?raw=true)

#### Inndividual Applicant Overview Dashboard 

![alt text](https://github.com/Data210/data210-final-project/blob/fe579e507d969f2cf8b52c38f5c0a79d2e56a499/Viz/MicrosoftTeams-image%20(1).png?raw=true)
