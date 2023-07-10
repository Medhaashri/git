import redshift_connector as rd

def write_data_to_redshift(table_name,bucket_name,path,access_key,secret_key):
    conn = rd.connect(
        host='social-analytics-redshift.chkbkhjojztp.us-west-2.redshift.amazonaws.com',
        port=5439,
        database='dev',
        user='socialanalyticsadmin',
        password='Passw0rdsocial'
    )

    cursor = conn.cursor()

    query = "COPY {table} FROM 's3://{bucket}/{path}' " \
            "credentials 'aws_access_key_id={access_key};aws_secret_access_key={secret_key}' " \
            "IGNOREHEADER 1 CSV " \
            "timeformat 'YYYY-MM-DDTHH:MI:SS';".format(
                                                        table=table_name,
                                                       bucket=bucket_name,
                                                       path=path,
                                                       access_key=access_key,
                                                       secret_key=secret_key
                                                       )

    print(query)

    cursor.execute(query)
    conn.commit()

    if(conn):
        conn.close()
