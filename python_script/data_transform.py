
def read_json():
    import pandas as pd
    import json

    #Load the JSON file into Snowflake
    df_json = pd.read_json("D:\\Colgate_training\\Snowflake_dbt\\data\\data.json")
    return df_json






