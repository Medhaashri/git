import requests
import json
import pandas as pd
from datetime import datetime
import s3_process
import io

def reach(config):
    url = f"{config['url']}{config['id']}{config['story_endpoint']}?access_token={config['access_token']}"
    final_df = pd.DataFrame()
    response = requests.get(url).json()
    for account in response['data']:
        df = pd.json_normalize(account)
        final_df = final_df.append(df, ignore_index=True)

    metric_values = []
    for index, row in final_df.iterrows():
        value = row['id']
        insight_url = f"{config['url']}{str(value)}{config['story_metrics_endpoint']}&access_token={config['access_token']}"
        response = requests.get(insight_url).json()
        metric_data = {metric['name']: metric['values'][0]['value'] for metric in response['data']}
        metric_values.append(metric_data)

    result_df = pd.DataFrame(metric_values)
    result_df = result_df.reindex(columns=['taps_forward', 'taps_back', 'replies', 'reach', 'exits', 'follows', 'total_interactions', 'shares'])
    result_df.fillna(0, inplace=True)  # Replace NaN with 0

    return result_df







        

if __name__ == "__main__":
    with open('/Users/medhaashriv/Documents/nbc/config_file_instagram.json', 'r') as a:
        config = json.load(a)
    result_df = reach(config['instagram'])
    result_df.to_csv('/Users/medhaashriv/Documents/nbc/stories.csv', index=False)

    with io.StringIO() as csv_buffer:
        result_df.to_csv(csv_buffer,header=True,index=False)
        s3_process.write_data_to_s3(data=csv_buffer.getvalue(),s3_path="instagram/{currentdate}/stories.csv".format(currentdate=datetime.today().date()),bucket_name="social-pulse")

    

    
