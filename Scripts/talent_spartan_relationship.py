def addApplicantID(talent_record,df_spartans,df_spartans_copy):
     result = df_spartans[(df_spartans['name'] == talent_record['name']) & (df_spartans['course_name'] == talent_record['course_interest']) &
                (df_spartans['date'] >= talent_record['date'])].sort_values(by='date',axis=0,ascending=True)
     if not result.empty:
          df_spartans_copy.at[result.index[0],'json_key'] = talent_record['json_key']
          print(talent_record['json_key'])
          print(result.index[0])