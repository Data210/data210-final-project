
df=df.drop(['month','year','month to use','nameuse'],axis=1)
problems = 0
for id in range(0,len(df['iddate'])):
    if len(df['iddate'][id]) ==7:
        word=df['iddate'][id]
        last_letter=df['iddate'][id][-1]
        word=word[:-1]
        df['iddate'][id]=word+'0'+last_letter
with open('applicants_clean.csv','w') as file:
    df.to_csv(file)
