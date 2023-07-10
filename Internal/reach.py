import requests
import json
import pandas as pd
from datetime import datetime
import s3_process
import io
import redshift


def reach(config):
    url = f"{config['url']}{config['id']}{config['url_to_get_id']}&access_token={config['access_token']}"
    final_df = pd.DataFrame(columns=['post_id', 'engagement', 'impression', 'reach', 'saved'])
    response = requests.get(url).json()
    

    for account in response['data']:
        account_id = account['id']
        insight_url = f"{config['url']}{account_id}{config['reach_endpoint']}&access_token={config['access_token']}"
        response = requests.get(insight_url).json()
        insights_data = {
            'post_id': account_id,
            'engagement': 0,
            'impression': 0,
            'reach': 0,
            'saved': 0
        }
        if 'error' in response:
            insights_data = {
                'post_id': account_id,
                'engagement': 'Media posted before business account conversion',
                'impression': 'Media posted before business account conversion',
                'reach': 'Media posted before business account conversion',
                'saved': 'Media posted before business account conversion'
            }
        else:
            for metric in response['data']:
                if metric['name'] in insights_data:
                    insights_data[metric['name']] = metric['values'][0]['value']
        final_df = final_df.append(insights_data, ignore_index=True)

    if final_df.empty:
        final_df = pd.DataFrame(columns=['post_id', 'engagement', 'impression', 'reach', 'saved'])

    with io.StringIO() as csv_buffer:
        final_df.to_csv(csv_buffer,header=True,index=False)
        s3_process.write_data_to_s3(data=csv_buffer.getvalue(),s3_path="instagram/{currentdate}/reach.csv".format(currentdate=datetime.today().date()),bucket_name="social-pulse")
        redshift.write_data_to_redshift('instagram.reach','social-pulse',"instagram/{currentdate}/reach.csv".format(currentdate=datetime.today().date()),config['aws_access_key'],config['aws_secret_key'])



    final_df.to_csv('/Users/medhaashriv/Documents/nbc/reach.csv', index=False)


if __name__ == "__main__":
    with open('/Users/medhaashriv/documents/nbc/config_file_instagram.json', 'r') as file:
        config = json.load(file)
    reach(config['instagram'])
