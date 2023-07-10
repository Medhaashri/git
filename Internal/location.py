import pandas as pd
import json
import requests
from datetime import datetime
import s3_process
import io
import redshift


def location(config):
    url = f"{config['url']}{config['id']}{config['location_endpoint']}&access_token={config['access_token']}"
    response = requests.get(url).json()
    data = response['data']
    
    gender_age_data = data[0]
    locale_data = data[1]
    country_data = data[2]
    city_data = data[3]
    
    gender_age_values = gender_age_data['values'][0]['value']
    locale_values = locale_data['values'][0]['value']
    country_values = country_data['values'][0]['value']
    city_values = city_data['values'][0]['value']

    city_df = pd.DataFrame.from_dict(city_values, orient='index').reset_index()
    city_df.columns = ['a','count']
    city_df[['city','state']] = city_df['a'].str.split(',', expand=True)
    city_df.drop('a', axis=1, inplace=True)
    
    gender_age_df = pd.DataFrame.from_dict(gender_age_values, orient='index').reset_index()
    gender_age_df.columns = ['gender_age', 'count']
    gender_age_df[['gender', 'age']] = gender_age_df['gender_age'].str.split('.', expand=True)
    gender_age_df['gender'] = gender_age_df['gender'].replace({'F': 'Female', 'M': 'Male', 'U': 'Unspecified'})
    gender_age_df.drop('gender_age', axis=1, inplace=True)


    locale_df = pd.DataFrame.from_dict(locale_values, orient='index').reset_index()
    locale_df.columns = ['locale', 'count']
    
    country_df = pd.DataFrame.from_dict(country_values, orient='index').reset_index()
    country_df.columns = ['country_code', 'count']
    country_names = {
        'AE': 'United Arab Emirates',
        'IN': 'India',
        'BH': 'Bahrain',
        'FR': 'France',
        'KW': 'Kuwait',
        'MY': 'Malaysia',
        'SA': 'Saudi Arabia',
        'BR': 'Brazil',
        'QA': 'Qatar',
        'AU': 'Australia',
        'SG': 'Singapore',
        'NG': 'Nigeria',
        'KG': 'Kyrgyzstan',
        'CA': 'Canada',
        'OM': 'Oman',
        'US': 'United States'
    }
    country_df['country'] = country_df['country_code'].map(country_names)
    
    return gender_age_df, locale_df, country_df ,city_df


if __name__ == "__main__":
    with open('/Users/medhaashriv/Documents/nbc/config_file_instagram.json', 'r') as a:
        config = json.load(a)
    gender_age_df, locale_df, country_df, city_df = location(config['instagram'])
    
    
    
        
    # with io.StringIO() as csv_buffer:
    #     gender_age_df.to_csv(csv_buffer,header=True,index=False)
    #     s3_process.write_data_to_s3(data=csv_buffer.getvalue(),s3_path="instagram/{currentdate}/age_gender.csv".format(currentdate=datetime.today().date()),bucket_name="social-pulse")
    #     redshift.write_data_to_redshift('instagram.age_gender','social-pulse',"instagram/{currentdate}/age_gender.csv".format(currentdate=datetime.today().date()),config['instagram']['aws_access_key'],config['instagram']['aws_secret_key'])
    # with io.StringIO() as csv_buffer:
    #     locale_df.to_csv(csv_buffer,header=True,index=False)
    #     s3_process.write_data_to_s3(data=csv_buffer.getvalue(),s3_path="instagram/{currentdate}/locale.csv".format(currentdate=datetime.today().date()),bucket_name="social-pulse")
    #     redshift.write_data_to_redshift('instagram.locale','social-pulse',"instagram/{currentdate}/locale.csv".format(currentdate=datetime.today().date()),config['instagram']['aws_access_key'],config['instagram']['aws_secret_key'])
    # with io.StringIO() as csv_buffer:
    #     country_df.to_csv(csv_buffer,header=True,index=False)
    #     s3_process.write_data_to_s3(data=csv_buffer.getvalue(),s3_path="instagram/{currentdate}/country.csv".format(currentdate=datetime.today().date()),bucket_name="social-pulse")
    #     redshift.write_data_to_redshift('instagram.country','social-pulse',"instagram/{currentdate}/country.csv".format(currentdate=datetime.today().date()),config['instagram']['aws_access_key'],config['instagram']['aws_secret_key'])

    with io.StringIO() as csv_buffer:
        city_df.to_csv(csv_buffer,header=True,index=False)
        s3_process.write_data_to_s3(data=csv_buffer.getvalue(),s3_path="instagram/{currentdate}/city.csv".format(currentdate=datetime.today().date()),bucket_name="social-pulse")
        redshift.write_data_to_redshift('instagram.city','social-pulse',"instagram/{currentdate}/city.csv".format(currentdate=datetime.today().date()),config['instagram']['aws_access_key'],config['instagram']['aws_secret_key'])

          









