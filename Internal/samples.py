import io
import redshift
import json
from datetime import datetime

def sample(config):
    redshift.write_data_to_redshift('instagram.followers','social-pulse',"instagram/2023-07-05/followers.csv",config['aws_access_key'],config['aws_secret_key'])




if __name__ == "__main__":
    with open('/Users/medhaashriv/documents/nbc/config_file_instagram.json', 'r') as file:
        config = json.load(file)
    sample(config['instagram'])







                                                                                                                                                                                                                                