import pandas as pd
import json
import requests
from datetime import datetime, timedelta
import s3_process
import io
import redshift


# getting the id's:
def fetch_data(config):
    url = f"{config['url']}{config['id']}{config['url_to_get_id']}&access_token={config['access_token']}"
    final_df = pd.DataFrame()
    response = requests.get(url).json()
    
    for account in response['data']:
        df = pd.json_normalize(account)
        final_df = final_df.append(df, ignore_index=True)
    
    return final_df


# getting data in historical load:
def fetch_historical(final_df, config):
    final_df2 = pd.DataFrame()
    
    for index, row in final_df.iterrows():
        value = row['id']
        insight_url = f"{config['url']}{str(value)}{config['Historyload_endpoint']}&access_token={config['access_token']}"
        response = requests.get(insight_url).json()
        df2 = pd.json_normalize(response)
        
        if (df2['media_product_type'] == 'REELS').bool() == True:
            reels_url = f"{config['url']}{str(value)}{config['reels_endpoint']}&access_token={config['access_token']}"
            reels_response = requests.get(reels_url).json()
            if 'data' in reels_response:
                df2['plays'] = reels_response['data'][0]['values'][0]['value']
                df2['total_interactions'] = reels_response['data'][1]['values'][0]['value']
            else:
                df2['plays'] = 'Media posted before business account conversion'
                df2['total_interactions'] = 'Media posted before business account conversion'
        else:
            df2['plays'] = None
            df2['total_interactions'] = None
        
        final_df2 = final_df2.append(df2, ignore_index=True)
    
    # Change column name from 'timestamp' to 'post_timestamp'
    final_df2 = final_df2.rename(columns={'timestamp': 'post_timestamp'})
    
    # Writing the output to the S3 bucket
    with io.StringIO() as csv_buffer:
        final_df2.to_csv(csv_buffer, header=True, index=False)
        s3_process.write_data_to_s3(data=csv_buffer.getvalue(), s3_path="instagram/{currentdate}/post_insights_historical.csv".format(currentdate=datetime.today().date()), bucket_name="social-pulse")
        redshift.write_data_to_redshift('instagram.post_insights_historical','social-pulse',"instagram/{currentdate}/post_insights_historical.csv".format(currentdate=datetime.today().date()),config['aws_access_key'],config['aws_secret_key'])

    
    final_df2.to_csv('/Users/medhaashriv/Documents/nbc/insights.csv')
    print("Historical data completed")


# getting the data in incremental load:
def fetch_incremental(final_df, config):
    final_df3 = pd.DataFrame()
    
    # Getting the id and timestamp
    for index, row in final_df.iterrows():
        value = row['id']
        insight_url = f"{config['url']}{str(value)}{config['incremental_endpoint']}&access_token={config['access_token']}"
        response = requests.get(insight_url).json()
        df2 = pd.json_normalize(response)
        
        # Filtering the timestamp
        df2['timestamp'] = pd.to_datetime(df2['timestamp'])
        filtered_df = df2[df2['timestamp'] > str(datetime.today().date() - timedelta(days=int(config['days'])))]
        
        # Getting the post insights
        for index, row in filtered_df.iterrows():
            value = row['id']
            url = f"{config['url']}{str(value)}{config['insights_url']}&access_token={config['access_token']}"
            response = requests.get(url).json()
            df3 = pd.json_normalize(response)
            
            if (df3['media_product_type'] == 'REELS').bool() == True:
                reels_url = f"{config['url']}{str(value)}{config['reels_endpoint']}&access_token={config['access_token']}"
                reels_response = requests.get(reels_url).json()
                if 'data' in reels_response:
                    df3['plays'] = reels_response['data'][0]['values'][0]['value']
                    df3['total_interactions'] = reels_response['data'][1]['values'][0]['value']
                else:
                    df3['plays'] = 'Media posted before business account conversion'
                    df3['total_interactions'] = 'Media posted before business account conversion'
            else:
                df3['plays'] = None
                df3['total_interactions'] = None
            
            final_df3 = final_df3.append(df3, ignore_index=True)
    
    # Change column name from 'timestamp' to 'post_timestamp'
    final_df3 = final_df3.rename(columns={'timestamp': 'post_timestamp'})
    
    # Writing the output to the S3 bucket
    with io.StringIO() as csv_buffer:
        final_df3.to_csv(csv_buffer, header=True, index=False)
        s3_process.write_data_to_s3(data=csv_buffer.getvalue(), s3_path="instagram/{currentdate}/post_insights_incremental.csv".format(currentdate=datetime.today().date()), bucket_name="social-pulse")
        # redshift.write_data_to_redshift('instagram.post_insights_incremental','social-pulse',"instagram/{currentdate}/post_insights_incremental.csv".format(currentdate=datetime.today().date()),config['aws_access_key'],config['aws_secret_key'])
        

    
    final_df3.to_csv('/Users/medhaashriv/Documents/nbc/insights_incremental.csv')
    print("Incremental data completed")


def main():
    with open('/Users/medhaashriv/Documents/nbc/config_file_instagram.json', 'r') as file:
        config = json.load(file)
        config = config['instagram']
    
    my_data = fetch_data(config)

    if config['days'].isnumeric():
        fetch_incremental(my_data, config)
    elif config['days'] == 'all' or config['days'] == 'All':
        fetch_historical(my_data, config)
    else:
        raise ValueError


if __name__ == "__main__":
    main()
