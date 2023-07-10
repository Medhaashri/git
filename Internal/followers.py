import pandas as pd
import json
import requests
from datetime import datetime, timedelta
import s3_process
import io
import redshift


def profile_views(config):
    url = f"{config['url']}{config['id']}{config['online_followers_endpoint']}&access_token={config['access_token']}"
    response = requests.get(url).json()

    if 'data' not in response or not response['data']:
        print('No data available')
        return None

    data_values = response['data'][0].get('values', [])

    if not data_values:
        print('No data available')
        return None

    df = pd.DataFrame(columns=['end_time'] + list(range(24)))

    for data in data_values:
        end_time = data['end_time']
        values = data['value']

        if not values:
            continue

        row = {'end_time': end_time}

        for hour in range(24):
            value = values.get(str(hour), None)
            row[hour] = value

        df = df.append(row, ignore_index=True)

    while 'previous' in response['paging']:
        next_url = response['paging']['previous']
        response = requests.get(next_url).json()

        if 'data' not in response or not response['data']:
            print('No data available in the second response')
            break

        data_values = response['data'][0].get('values', [])
        
        if not data_values:
            print('No data available in the second response')
            break

        for data in data_values:
            end_time = data['end_time']
            values = data['value']

            if not values:
                continue

            row = {'end_time': end_time}

            for hour in range(24):
                value = values.get(str(hour), None)
                row[hour] = value

            df = df.append(row, ignore_index=True)

    if df.empty:
        print('No data available')
        return None

    # Remove unnecessary columns and rows
    df = df.dropna().reset_index(drop=True)

    # Convert end_time to datetime format
    df['end_time'] = pd.to_datetime(df['end_time'], format='%Y-%m-%dT%H:%M:%S+0000')

    # Calculate the date range for the last 30 days excluding today and yesterday
    end_date = datetime.now() - timedelta(days=2)
    start_date = end_date - timedelta(days=30)
    
    # Filter data for the specified date range
    df = df[(df['end_time'].dt.date >= start_date.date()) & (df['end_time'].dt.date <= end_date.date())]

    # Sort the dataframe by end_time in ascending order
    df = df.sort_values('end_time', ascending=True)

    # Transpose rows to create a new dataframe
    new_df = pd.DataFrame(columns=['Date', 'Hours', 'Value'])
    
    for index, row in df.iterrows():
        date = row['end_time']
        values = row.drop('end_time').values.tolist()

        for hour, value in enumerate(values):
            new_df = new_df.append({'Date': date, 'Hours': hour, 'Value': value}, ignore_index=True)

    with io.StringIO() as csv_buffer:
        new_df.to_csv(csv_buffer, header=True, index=False)
        s3_process.write_data_to_s3(data=csv_buffer.getvalue(),
                                    s3_path="instagram/{currentdate}/online_followers.csv".format(currentdate=datetime.today().date()),
                                    bucket_name="social-pulse")
        redshift.write_data_to_redshift('instagram.online_followers','social-pulse',"instagram/{currentdate}/followers.csv".format(currentdate=datetime.today().date()),config['aws_access_key'],config['aws_secret_key'])


    new_df.to_csv('/Users/medhaashriv/Documents/nbc/online_followers.csv', index=False)


if __name__ == "__main__":
    with open('/Users/medhaashriv/Documents/nbc/config_file_instagram.json', 'r') as a:
        config = json.load(a)
        profile_views(config['instagram'])
