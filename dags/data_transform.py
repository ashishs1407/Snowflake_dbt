
def read_json_file():
    import pandas as pd
  
    #Load the JSON file into Snowflake
    df_json = pd.read_json("//opt//airflow//data//data.json")
    return df_json
    










