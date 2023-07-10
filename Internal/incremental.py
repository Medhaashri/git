import pandas as pd
import json
import requests
from datetime import datetime, timedelta
# import s3_process
# import io


#reading the config file
with open ('/Users/medhaashriv/Documents/nbc/configure.json','r') as a:
    config=json.load(a)



#getting the post id's to get the timestamp:
def incremental():
    insight_url=f"{config['instagram'].get('url')}{config['instagram'].get('incremental_endpoint')}&access_token={config['instagram'].get('access_token')}"
    response = requests.get(insight_url).json()
    df2 = pd.json_normalize(response)
    final_df2 = pd.DataFrame()
    final_df2=final_df2.append(df2,ignore_index=True)
    print(final_df2)
    
    
#getting post on required date:
    final_df2['timestamp'] = pd.to_datetime(final_df2['timestamp'])
    filtered_df = final_df2[final_df2['timestamp'] > str(datetime.today().date() - timedelta(days=int(config['instagram'].get('days').split("=")[1])))]
    for index,row in filtered_df.iterrows():
        value= row['id']
        url=f"{config['instagram'].get('url')}{str(value)}{config['instagram'].get('insights_url')}&access_token={config['instagram'].get('access_token')}"
        response = requests.get(insight_url).json()
        df3=pd.json_normalize(response)
        final_df2=final_df2.append(df3,ignore_index=True)
    final_df2.to_csv('/Users/medhaashriv/Documents/nbc/insights_incremental.csv')



#for getting post on historical load:
def historical():
    url=f"{config['instagram'].get('url')}{config['instagram'].get('id')}{config['instagram'].get('url_to_get_id')}&access_token={config['instagram'].get('access_token')}"
    final_df = pd.DataFrame()
    response = requests.get(url).json()
    for account in response['data']:
        df = pd.json_normalize(account)
        final_df=final_df.append(df,ignore_index=True)
    final_df2 = pd.DataFrame()

#getting the post insights using the post id's:
    for index,row in final_df.iterrows():
        value = row['id']
        insight_url=f"{config['instagram'].get('url')}{str(value)}{config['instagram'].get('Historyload_endpoint')}&access_token={config['instagram'].get('access_token')}"
        response = requests.get(insight_url).json()
        df2 = pd.json_normalize(response)
        final_df2=final_df2.append(df2,ignore_index=True)
    final_df2.to_csv('/Users/medhaashriv/Documents/nbc/insights.csv')

    


    
#writting the output in to s3 bucket:
    # with io.StringIO() as csv_buffer:
    #     final_df2.to_csv(csv_buffer,header=True,index=False)
    # s3_process.write_data_to_s3(data=csv_buffer.getvalue(),s3_path="instagram/currentdate/post_insights.csv",bucket_name="nithin-first-bucket")


if __name__=="__main__":
    filtered_response=incremental()