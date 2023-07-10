import pandas as pd
import json
import requests

with open ('/Users/medhaashriv/Documents/nbc/configure.json','r') as a:
    config=json.load(a)

def get_response():
    url=f"{config['instagram'].get('url')}{config['instagram'].get('id')}{config['instagram'].get('url_to_get_id')}&access_token={config['instagram'].get('access_token')}"
    final_df = pd.DataFrame()
    response = requests.get(url).json()
    for account in response['data']:
        df = pd.json_normalize(account)
        final_df=final_df.append(df,ignore_index=True)

    final_df.to_csv('/Users/medhaashriv/Documents/nbc/post_id.csv')


if __name__=="__main__":
    filtered_response=get_response()