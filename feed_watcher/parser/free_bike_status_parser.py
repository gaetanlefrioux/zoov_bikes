import pandas as pd

def parse(json_content):
    bikes_df = pd.json_normalize(json_content["data"]["bikes"])
    bikes_df["last_updated"] = json_content["last_updated"]

    return bikes_df
