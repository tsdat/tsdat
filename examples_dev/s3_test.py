# Test reading and writing files from an S3 bucket using boto3 library.
import boto3
import io
import os
import tempfile
import zipfile
import tarfile
from datetime import datetime

bucket_name = 'mhk-datalake-test'


def get_bucket(bucketname, region_name, access_key_id, access_key):
    """
    This example shows how to get an API object for
    the given bucket that is located in a different
    region and/or using different credentials than
    the default configured region/credentials
    """
    botoSession = boto3.Session(
        aws_access_key_id=access_key_id,
        aws_secret_access_key=access_key,
        region_name=region_name,
    )

    s3 = botoSession.resource('s3')
    bucket = s3.Bucket(bucketname)
    return bucket


def upload_local_file_to_s3(local_file_path, s3_path):
    """
    This function demonstrates how to upload a local file to an s3 bucket.
    Note that the region, and access token used come from the local
    aws configuration.  You can find your region in the ~/.aws/config
    file and your key in the ~/.aws/credentials file.

    # Note that s3 path should NOT start with slash, since it is
    # relative to the bucket root
    """
    s3_client = boto3.client('s3')

    # s3_client is lower level than the resource objects which use
    # a higher level API, and it gives better upload performance
    with open(local_file_path, "rb") as f:
        s3_client.upload_fileobj(f, bucket_name, s3_path)

    print(f'file uploaded: {s3_path}')


def delete(s3_path):
    # First delete this resource
    s3 = boto3.resource('s3')
    s3.Object(bucket_name, s3_path).delete()

    # Then, delete any children if they exist
    prefix = s3_path
    prefix = f'{prefix}/' if not prefix.endswith('/') else prefix
    bucket = s3.Bucket(bucket_name)
    objects = bucket.objects.filter(Prefix=prefix)
    objects.delete()


def delete_files_in_bucket(prefix):
    """
    Delete all the files in a bucket starting with the
    given prefix
    """
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(bucket_name)
    objects = bucket.objects.filter(Prefix=prefix)
    objects.delete()
    print(f'Files deleted from {prefix}')


def untar_file_in_s3(tar_s3_path, dest_s3_folder_path):
    """
    This function demonstrates how to untar a file in one s3 folder
    into another temporary s3 folder in memory without using local disk.
    """
    s3_resource = boto3.resource('s3')
    tar_obj = s3_resource.Object(bucket_name=bucket_name, key=tar_s3_path)
    buffer = io.BytesIO(tar_obj.get()["Body"].read())

    tar = tarfile.open(fileobj=buffer, mode='r')

    for member in tar.getmembers():
        dest_path = os.path.join(dest_s3_folder_path, member.name)
        file_obj = tar.extractfile(member)

        if file_obj: #  file_obj will be None if it is a folder

            # Note that on windows, os.path.join forces the use of \
            # separator :(, so we have to replace it because
            # s3 paths get all messed up if you use the \ separator!
            dest_path = dest_path.replace("\\", "/")

            print(f'Extracting file: {dest_path}')

            s3_resource.meta.client.upload_fileobj(
                file_obj,
                Bucket=bucket_name,
                Key=f'{dest_path}'
            )

    print('Untar successful')


def unzip_file_in_s3(zip_s3_path, dest_s3_folder_path):
    """
    This function demonstrates how to unzip a file in one s3 folder
    into another temporary s3 folder in memory without using local disk.
    """
    s3_resource = boto3.resource('s3')
    zip_obj = s3_resource.Object(bucket_name=bucket_name, key=zip_s3_path)
    buffer = io.BytesIO(zip_obj.get()["Body"].read())

    z = zipfile.ZipFile(buffer)
    for filename in z.namelist():
        dest_path = os.path.join(dest_s3_folder_path, filename)

        # Note that on windows, os.path.join forces the use of \
        # separator :(, so we have to replace it because
        # s3 paths get all messed up if you use the \ separator!
        dest_path = dest_path.replace("\\", "/")

        print(f'Extracting file: {dest_path}')
        file_info = z.getinfo(filename)
        s3_resource.meta.client.upload_fileobj(
            z.open(filename),
            Bucket=bucket_name,
            Key=f'{dest_path}'
        )

    print('Unzip successful')


def fetch(bucket_path):
    s3_client = boto3.client('s3')
    s3_client.download_file(bucket_name, bucket_path, 'data/fetched_file.csv')


def exists(s3_path):
    s3 = boto3.resource('s3')
    s3.Object(bucket_name, s3_path).load()


def parse_aws_path(aws_path: str):
    parts = aws_path.split("$$$")
    if len(parts) == 2:
        parts.insert(0, None)
    return tuple(parts)


def list_files(bucket_path):
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(bucket_name)

    prefix = bucket_path
    prefix = f'{prefix}/' if not prefix.endswith('/') else prefix

    for file in bucket.objects.filter(Prefix=prefix, Delimiter='/'):
        print(file.key)


#upload_local_file_to_s3('data/maraosmetM1.a1.20180201.000000.nc', 'mar/maraosmetM1.a1/maraosmetM1.a1.20180201.000000.nc')
#delete('mar/maraosmetM1.a1/maraosmetM1.a1.20180201.000000.nc')

#delete_files_in_bucket('tsdat/tmp')

#upload_local_file_to_s3('data/buoy.z05.00.20201004.000000_no_gill_waves.zip', 'tsdat/raw/buoy.z05.00.20201004.000000_no_gill_waves.zip')
#unzip_file_in_s3('tsdat/raw/buoy.z05.00.20201004.000000_no_gill_waves.zip', 'tsdat/tmp')

#delete_files_in_bucket('tsdat/tmp/__MACOSX')
#fetch('tsdat/tmp/buoy.z05.00.20201004.000000.currents.csv')

#path = 'us-west-2$$$mhk-datalake-test$$$tsdat/raw/buoy.z05.00.20201004.000000_no_gill_waves.zip'

#upload_local_file_to_s3('data/buoy.z05.00.20201004.000000_no_gill_waves.tar.gz', 'tsdat/raw/buoy.z05.00.20201004.000000_no_gill_waves.tar.gz')
#untar_file_in_s3('tsdat/raw/buoy.z05.00.20201004.000000_no_gill_waves.tar.gz', 'tsdat/tmp/tar')

#list_files('tsdat/tmp/tar')

delete('tsdat/tmp')
delete('tsdat/raw')

print('done')



