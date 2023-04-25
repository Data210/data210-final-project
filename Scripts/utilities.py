import pandas as pd
def splitAndRemap(target_df,map_dfs,key_column, match_columns):
    mapping = dict(pd.concat(map_dfs)[[match_columns,key_column]].values.tolist())
    target_df[match_columns] = target_df[match_columns].map(mapping)
    return target_df.rename(columns={match_columns : key_column})

def checkNewRecords(new_df,current_df,id_column):
    if id_column:
        merge_df = current_df.drop(id_column,axis=1)
    else:
        merge_df = current_df
    try:
        new_df = new_df.merge(merge_df, how="left", indicator=True)\
            .query("_merge == 'left_only'").drop('_merge',axis=1).reset_index(drop=True)
    except ValueError:
        new_df = new_df.astype(dict(zip(current_df.columns.to_list(),current_df.dtypes.to_list())))
        new_df = new_df.merge(merge_df, how="left", indicator=True)\
            .query("_merge == 'left_only'").drop('_merge',axis=1).reset_index(drop=True)
    if id_column:
        new_df[id_column] = pd.RangeIndex(len(new_df)) + (current_df['applicant_id'].max() if not pd.isna(float(current_df['applicant_id'].max())) else -1) + 1
    return new_df