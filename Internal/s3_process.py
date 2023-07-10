import boto3
import botocore
import os
import sys
from botocore.exceptions import ClientError


s3_credentials = dict()


def set_bucket_name(s3_bucket):
    s3_credentials["s3_bucket"] = s3_bucket


def read_data_from_s3(s3_path, bucket_name=""):
    try:
        if bucket_name =="":
            bucket_name = s3_credentials.get("s3_bucket")
        aws_mang_con = boto3.session.Session()
        s3_client = aws_mang_con.client(service_name="s3", region_name="us-east-1")
        object_s3 = s3_client.get_object(Bucket=bucket_name, Key=s3_path)

        if object_s3["ResponseMetadata"]["HTTPStatusCode"] == 200:
            print("Reading data from s3 successful")
            file_data = object_s3['Body'].read()
            contents = file_data.decode('utf-8')
            return contents
        else:
            print("Reading data from  s3 failed")
            return False
    except Exception as err:
        print(f"Exception in read data from s3{err}")
        return False



def write_data_to_s3(data, s3_path, bucket_name=""):
    # Copies the data in argument to an s3 path
    try:
        if bucket_name == "":
            bucket_name = s3_credentials.get("s3_bucket")
        session = boto3.session.Session()
        s3 = session.resource('s3')

        object_s3 = s3.Object(bucket_name, s3_path)

        result = object_s3.put(Body=str(data))
        if result["ResponseMetadata"]["HTTPStatusCode"] == 200:
            print(f"Writing data to s3 {bucket_name}/{s3_path} successful")
            return True
        else:
            print(f"Writing data to s3 {s3_path} failed")
            return False
    except Exception as err:
        print(f"Exception in write dat to s3{err}")
        return False


def cp_local_to_s3(local_path, s3_path):

    # Copies the local file to an s3 path
    try:
        session = boto3.session.Session()
        s3 = session.resource('s3')

        s3.Bucket(s3_credentials.get("s3_bucket")).upload_file(local_path, s3_path)
        print(f"cp_local_to_s3: Upload file {s3_path} successful")
        return True
    except botocore.exceptions.ClientError as error:
        raise error
    except botocore.exceptions.ParamValidationError as error:
        raise ValueError('The parameters you provided are incorrect: {}'.format(error))


def copy_s3_to_s3(source_bucket, source_file, target_bucket, target_file):
    # Creating the connection with the resource
    session = boto3.session.Session()
    s3 = session.resource('s3')

    # Declaring the source to be copied
    copy_source = {
        'Bucket': source_bucket,
        'Key': source_file
    }

    s3.meta.client.copy(copy_source, target_bucket, target_file)


def delete_from_s3(path, bucket_name=""):
    try:
        session = boto3.session.Session()
        s3_resource = session.resource("s3")
        if bucket_name == "":
            bucket_name = s3_credentials.get("s3_bucket")

        response = s3_resource.Bucket(bucket_name)

        # Delete from s3 playlist id path
        for obj in response.objects.filter(Prefix=path):
            if obj.key[-1] != '/':
                print(obj)
                obj.delete()
        print("Files deleted successfully")
        return True
    except ClientError as delete_err:
        exception_type, exception_object, exception_traceback = sys.exc_info()
        filename = os.path.basename(exception_traceback.tb_frame.f_code.co_filename)
        line_number = exception_traceback.tb_lineno
        raise Exception(f"{filename}:LineNo-{line_number}:Error-{str(delete_err)}")


def is_directory_empty(s3_path, ignore_prefix=True):
    session = boto3.session.Session()
    s3 = session.resource("s3")
    my_bucket = s3.Bucket(s3_credentials.get("s3_bucket"))
    list_objects = my_bucket.objects.filter(Prefix=s3_path)
    print('list_object:', list_objects)
    count = 0
    file_list = []
    for obj in list_objects:
        count = count + 1
        file_list.append(obj.key)
    return count, file_list


if __name__ == '__main__':
    set_bucket_name("nithin-first-bucket")
    is_directory_empty("NBC-external/")

                      