import pandas as pd
import json
import requests
from datetime import datetime
import s3_process
import io


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
    with open('/Users/medhaashriv/Documents/nbc/configure.json', 'r') as a:
        config = json.load(a)
    gender_age_df, locale_df, country_df, city_df = location(config['instagram'])
    
    gender_age_df.to_csv('/Users/medhaashriv/Documents/nbc/age_gender.csv')
    locale_df.to_csv('/Users/medhaashriv/Documents/nbc/locale.csv')
    country_df.to_csv('/Users/medhaashriv/Documents/nbc/country.csv')
    city_df.to_csv('/Users/medhaashriv/Documents/nbc/city.csv')
    
    







