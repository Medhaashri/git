# import pandas as pd
# import json
# import requests
# from datetime import datetime
# import s3_process
# import io


# def profile_views(config):
#     url = f"{config['url']}{config['id']}{config['views_endpoint']}&access_token={config['access_token']}"
#     response = requests.get(url).json()
    
    
#     # Extract the initial values from the response


#     data_values = response['data'][0]['values']
    
        
    
#     # Create a dataframe of initial data_values
#     df = pd.DataFrame(data_values)
    
#     # Check if there are more pages of data
#     while 'previous' in response['paging']:
#         next_url = response['paging']['previous']
#         response = requests.get(next_url).json()
#         print(response)
        
       
        
#         # Extract the values from the current response
       
#         data_values = response['data'][0]['values']
        
        
#         # Append the new values to the dataframe
#         df = df.append(data_values, ignore_index=True)
#         # print(df)
        
#     with io.StringIO() as csv_buffer:
#         df.to_csv(csv_buffer,header=True,index=False)
#         s3_process.write_data_to_s3(data=csv_buffer.getvalue(),s3_path="instagram/{currentdate}/profile_views.csv".format(currentdate=datetime.today().date()),bucket_name="social-pulse")
#     df.to_csv('/Users/medhaashriv/Documents/nbc/profile_views.csv', index=False)

# if __name__ == "__main__":
    
#     # config = json.loads(s3_process.read_data_from_s3("config_files/config_file_instagram.json","social-pulse"))
#     # profile_views(config['instagram'])
#     with open ('/Users/medhaashriv/Documents/nbc/config_file_instagram.json','r') as a:
#         config=json.load(a)
#         profile_views(config['instagram'])

# import pandas as pd
# import json
# import requests
# from datetime import datetime
# import s3_process
# import io
# import redshift


# def profile_views(config):
#     url = f"{config['url']}{config['id']}{config['views_endpoint']}&access_token={config['access_token']}"
#     response = requests.get(url).json()
    
#     data_values = response['data'][0]['values']
    
#     df = pd.DataFrame(data_values)
    
#     while 'previous' in response['paging']:
#         next_url = response['paging']['previous']
#         response = requests.get(next_url).json()
        
#         if 'data' not in response:
#             print('No data available in the second response')
#             df = df.append(['No data'] * len(df.columns))
#             break
        
#         data_values = response['data'][0]['values']
        
#         df = df.append(data_values, ignore_index=True)
    
#     if len(df) == 0:
#         print('No data available')
#         return None
    
#     with io.StringIO() as csv_buffer:
#         df.to_csv(csv_buffer, header=True, index=False)
#         s3_process.write_data_to_s3(data=csv_buffer.getvalue(), s3_path="instagram/{currentdate}/profile_views.csv".format(currentdate=datetime.today().date()), bucket_name="social-pulse")
#         redshift.write_data_to_redshift('instagram.profile_views','social-pulse',"instagram/{currentdate}/profile_views.csv".format(currentdate=datetime.today().date()),config['aws_access_key'],config['aws_secret_key'])

    
#     df.to_csv('/Users/medhaashriv/Documents/nbc/profile_views.csv', index=False)

# if __name__ == "__main__":
#     with open('/Users/medhaashriv/Documents/nbc/config_file_instagram.json', 'r') as a:
#         config = json.load(a)
#         profile_views(config['instagram'])

import pandas as pd
import json
import requests
from datetime import datetime
import s3_process
import io
import redshift


def profile_views(config):
    url = f"{config['url']}{config['id']}{config['views_endpoint']}&access_token={config['access_token']}"
    response = requests.get(url).json()

    data_values = response['data'][0]['values']

    df = pd.DataFrame(data_values)

    while 'previous' in response['paging']:
        next_url = response['paging']['previous']
        response = requests.get(next_url).json()

        if 'data' not in response:
            print('No data available in the second response')
            df = df.append(['No data'] * len(df.columns))
            break

        data_values = response['data'][0]['values']

        df = df.append(data_values, ignore_index=True)

    if len(df) == 0:
        print('No data available')
        return None

    # Remove unnecessary columns and rows
    df = df[['end_time', 'value']].dropna().reset_index(drop=True)

    with io.StringIO() as csv_buffer:
        df.to_csv(csv_buffer, header=True, index=False)
        s3_process.write_data_to_s3(data=csv_buffer.getvalue(), s3_path="instagram/{currentdate}/profile_views.csv".format(currentdate=datetime.today().date()), bucket_name="social-pulse")
        redshift.write_data_to_redshift('instagram.profile_views','social-pulse',"instagram/{currentdate}/profile_views.csv".format(currentdate=datetime.today().date()),config['aws_access_key'],config['aws_secret_key'])

    df.to_csv('/Users/medhaashriv/Documents/nbc/profile_views.csv', index=False)

if __name__ == "__main__":
    with open('/Users/medhaashriv/Documents/nbc/config_file_instagram.json', 'r') as a:
        config = json.load(a)
        profile_views(config['instagram'])
