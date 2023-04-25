import sqlalchemy as db
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Date, ForeignKey, Boolean, PrimaryKeyConstraint
from sqlalchemy import text
from sqlalchemy_utils import database_exists, create_database
from dotenv import load_dotenv
from connection_string import create_connection_string

load_dotenv()

connect_string = create_connection_string()

def create_database():
    print('Creating Engine...')
    # engine = create_engine("mssql+pyodbc://admin:spartaglobal@project-testing.cjxe5m4vhofo.eu-west-2.rds.amazonaws.com:1433/project?driver=ODBC+Driver+17+for+SQL+Server")
    try:
        engine = create_engine(connect_string)
    except:
        return print('Error: Connection String Failed')
    print('\rDone!')

    try:
        if database_exists(engine.url) == False:
            create_database(engine.url)
    except:
        create_database(engine.url)
    # drop all tables
    metadata = db.MetaData()
    metadata.reflect(bind=engine)
    metadata.drop_all(bind=engine)
    print('Creating Schema...')
    with engine.connect() as conn:
        metadata = MetaData()

        #define tables
        table_name = 'Postcode'
        postcode_table = Table (table_name, metadata,
                                Column('postcode_id', Integer, primary_key=True, autoincrement=True),
                                Column('postcode', String)
                                )
        
        table_name = 'City'
        city_table = Table (table_name, metadata,
                                Column('city_id', Integer, primary_key=True, autoincrement=True),
                                Column('city', String)
                                )
        
        table_name = 'Address'
        address_table = Table (table_name, metadata,
                                Column('address_id', Integer, primary_key=True, autoincrement=True),
                                Column('address', String),
                                Column('postcode_id', Integer, ForeignKey('Postcode.postcode_id')),
                                Column('city_id', Integer, ForeignKey('City.city_id'))   
                                )
        
        table_name = 'University'
        university_table = Table (table_name, metadata,
                                Column('uni_id', Integer, primary_key=True, autoincrement=True),
                                Column('uni', String)
                                )
        
        table_name = 'Degree'
        degree_table = Table (table_name, metadata,
                                Column('degree_id', Integer, primary_key=True, autoincrement=True),
                                Column('degree', String)
                                )
        
        table_name = 'Personal_Details'
        personal_details_table = Table(table_name, metadata,
                            Column('person_id', Integer, primary_key=True, autoincrement=True),
                            Column('name', String),
                            Column('gender', String),
                            Column('dob', Date),
                            Column('email', String),
                            Column('phone_number', String),
                            Column('address_id', Integer, ForeignKey('Address.address_id')),
                            Column('degree_id', Integer, ForeignKey('Degree.degree_id')),
                            Column('uni_id', Integer, ForeignKey('University.uni_id'))                
                            )
        
        table_name = 'Recruiter'
        recruiter_table = Table (table_name, metadata,
                                Column('recruiter_id', Integer, primary_key=True, autoincrement=True),
                                Column('recruiter_name', String)
                                )
        
        table_name = 'Applicant'
        applicant_table = Table(table_name, metadata,
                            Column('applicant_id', Integer, primary_key=True, autoincrement=True),
                            Column('invited_date', Date),
                            Column('person_id', Integer, ForeignKey('Personal_Details.person_id')),
                            Column('recruiter_id', Integer, ForeignKey('Recruiter.recruiter_id')),
                            Column('sparta_day_result_id', Integer, ForeignKey('Sparta_Day_Result.sparta_day_result_id'))                
                            )
        
        table_name = 'Academy_Location'
        Academy_location_table = Table (table_name, metadata,
                            Column('academy_id', Integer, primary_key=True, autoincrement=True),
                            Column('academy', String)
                            )
        
        table_name = 'Sparta_Day'
        Sparta_Day_table = Table (table_name, metadata,
                            Column('sparta_day_id', Integer, primary_key=True, autoincrement=True),
                            Column('sparta_day_date', String),
                            Column('academy_id', Integer, ForeignKey('Academy_Location.academy_id'))
                            )
        
        table_name = 'Sparta_Day_Result'
        Sparta_Day_Result_table = Table (table_name, metadata,
                            Column('sparta_day_result_id', Integer, primary_key=True, autoincrement=True),
                            Column('psychometric_result', Integer),
                            Column('presentation_result', Integer),
                            Column('sparta_day_id', Integer, ForeignKey('Sparta_Day.sparta_day_id'))
                            )
        
        table_name = 'Stream'
        Stream_table = Table (table_name, metadata,
                            Column('stream_id', Integer, primary_key=True, autoincrement=True),
                            Column('stream', String)
                            )
        table_name = 'Talent'
        talent_table = Table(table_name, metadata,
                                    Column('talent_id', Integer, primary_key=True),
                                    Column('applicant_id', Integer, ForeignKey('Applicant.applicant_id')),
                                    Column('self_development', Boolean),
                                    Column('financial_support_self', Boolean),
                                    Column('pass', Boolean),
                                    Column('stream_id', Integer,ForeignKey('Stream.stream_id')),
                                    Column('geo_flex', Boolean)
                                    )
        table_name = 'Strength'
        strength_table = Table(table_name, metadata,
                            Column('strength_id', Integer, primary_key=True, autoincrement=True),
                            Column('strength', String(30), unique=True, nullable=False)
                            )
        table_name = 'Weakness'
        weakness_table = Table(table_name, metadata,
                            Column('weakness_id', Integer, primary_key=True, autoincrement=True),
                            Column('weakness', String(30), unique=True, nullable=False)
                            )
        table_name = 'Strength_Junction'
        strength_junction = Table(table_name,metadata,
                                Column('talent_id', Integer, ForeignKey('Talent.talent_id')),
                                Column('strength_id', Integer, ForeignKey('Strength.strength_id'))
                                )
        table_name = 'Weakness_Junction'
        weakness_junction = Table(table_name,metadata,
                                Column('talent_id', Integer, ForeignKey('Talent.talent_id')),
                                Column('weakness_id', Integer)
                                )
        table_name = 'Technology'
        tech_table = Table(table_name, metadata,
                        Column('tech_id', Integer, primary_key=True, autoincrement=True),
                        Column('tech_name', String(50), unique=True, nullable=False)
                        )
        table_name = 'Tech_Junction'
        tech_junction_table = Table(table_name, metadata,
                                    Column('tech_id', Integer, ForeignKey("Technology")),
                                    Column('talent_id', Integer, ForeignKey('Talent.talent_id')),
                                    Column('score', Integer, nullable=False)
                                    )
        table_name = 'Trainer'
        trainer_table = Table(table_name, metadata,
                                Column('trainer_id', Integer, primary_key=True),
                                Column('trainer', String(50), nullable=False, unique=True)
                                )
        table_name = 'Course'
        course_table = Table(table_name, metadata,
                            Column('course_id', Integer, primary_key=True),
                            Column('stream_id', Integer, ForeignKey('Stream.stream_id')),
                            Column('course_num', String),
                            Column('start_date', Date)
                            )
        table_name = 'Spartan'
        spartan_table = Table(table_name, metadata,
                            Column('spartan_id', Integer, primary_key=True, autoincrement=True),
                            Column('talent_id', Integer, ForeignKey('Talent.talent_id'), unique=True, nullable=True),
                            Column('trainer_id', Integer, ForeignKey('Trainer.trainer_id'), nullable=False),
                            Column('course_id', Integer, ForeignKey('Course.course_id'), nullable=False),
                            )
        table_name = 'Behaviour'
        behaviour_table = Table(table_name, metadata, 
                                Column('spartan_id', Integer, ForeignKey('Spartan.spartan_id')), 
                                Column('week', Integer), 
                                Column('analytic', Integer), 
                                Column('independent', Integer), 
                                Column('imaginative', Integer), 
                                Column('studious', Integer), 
                                Column('professional', Integer), 
                                Column('determined', Integer),
                                PrimaryKeyConstraint('spartan_id', 'week')
                                )
        metadata.create_all(engine)
    print('\rDone!')
    print('Loading and transforming data...')
    return engine