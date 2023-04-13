import applicant_details_transformation_v2
from s3 import S3Client
import pandas as pd
import re
import string
import io
from datetime import datetime, timezone
from dateutil.parser import parse
# now = datetime.now()




def Update_Data(time):
    dt=parse(time)
    output= applicant_details_transformation_v2.process_data_since(dt)

    if len(output) == 3:
        main_df, location_df, recruiter_df = output
        return main_df, location_df, recruiter_df
    elif len(output) == 15:
        print(output)
    else:
        print("Unexpected")

Update_Data("April 4, 2022, 13:42:56")
