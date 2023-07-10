import pandas as pd
import json
import requests

with open ('/Users/medhaashriv/Documents/nbc/configure.json','r') as a:
    config=json.load(a)


def get_response():
    url=f"{config['instagram'].get('url')}17841448707448315?fields=business_discovery.username(agilisium){config['instagram'].get('parameters')}&access_token={config['instagram'].get('access_token')}"
    final_df = pd.DataFrame()
    response = requests.get(url).json()
    for account in response['business_discovery']['media']['data']:
        df = pd.json_normalize(account)
        final_df=final_df.append(df,ignore_index=True)
        #df.to_csv('/Users/medhaashriv/Documents/nbc/sample_data_1.csv')
        # stats_to_keep={'like_count':None,'caption':'','comments_count':'','media_product_type':'','media_type':'','media_url':'','permalink':'','id':''}
        # for k in stats_to_keep.keys():
        #     stats_to_keep[k]=account.get(k)
    
    final_df.to_csv('/Users/medhaashriv/Documents/nbc/sample_data_1.csv')
                 
        
        #empty.append(info)
    #return empty

# def data_frame_conversion(my_dataframe):
#     df=pd.json_normalize(my_dataframe)
#     df.to_csv('/Users/medhaashriv/Documents/nbc/sample_data.csv')
    

    
if __name__=="__main__":
    filtered_response=get_response()
    #result_df=data_frame_conversion(filtered_response)







   
