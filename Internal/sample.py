import pandas as pd
import json
import requests
from datetime import datetime, timedelta


class Execute:
    def __init__(self):
        with open ('/Users/medhaashriv/Documents/nbc/configure.json','r') as a:
            config=json.load(a)

    def my_decorator(self,func):
        def wrapper():
            with open ('/Users/medhaashriv/Documents/nbc/configure.json','r') as a:
                config=json.load(a)
            url=f"{config['url']}{config['id']}{config['url_to_get_id']}&access_token={config['access_token']}"
            final_df = pd.DataFrame()
            response = requests.get(url).json()
            for account in response['data']:
                df = pd.json_normalize(account)
                final_df=final_df.append(df,ignore_index=True)
            final_df2 = pd.DataFrame()
            for index,row in final_df.iterrows():
                value = row['id']
                insight_url=f"{config['url']}{str(value)}{config['Historyload_endpoint']}&access_token={config['access_token']}"
                response = requests.get(insight_url).json()
                df2 = pd.json_normalize(response)
                final_df2=final_df2.append(df2,ignore_index=True)
                final_df2.to_csv('/Users/medhaashriv/Documents/nbc/insights.csv')
            return wrapper
        
    @my_decorator
    def my_function():
        df2['timestamp'] = pd.to_datetime(df2['timestamp'])
        filtered_df = df2[df2['timestamp'] > str(datetime.today().date() - timedelta(days=int(config['days'])))]
        for index,row in filtered_df.iterrows():
            value= row['id']
            url=f"{config['url']}{str(value)}{config['insights_url']}&access_token={config['access_token']}"
            response = requests.get(url).json()
            df3=pd.json_normalize(response)
            final_df3=final_df3.append(df3,ignore_index=True)
        return final_df3.to_csv('/Users/medhaashriv/Documents/nbc/insights_incremental.csv')
            
    

data_fetcher = Execute()

