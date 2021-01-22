import os
import abc
import shutil
import tempfile
import zipfile
import tarfile
import datetime
import boto3
from typing import List, Dict
from tsdat.io import DatastreamStorage


class AwsStorage(DatastreamStorage):

    @staticmethod
    def get_s3_path(region: str, bucket_name: str = None, key: str = None):
        """-------------------------------------------------------------------
        We are creating our own string to hold the region, bucket & key, since
        boto3 needs all three in order to access a file
        s3_client = boto3.client('s3', region_name='eu-central-1')
        s3_client.download_file(bucket, key, download_path)
        -------------------------------------------------------------------"""
        assert region
        assert bucket_name
        assert key
        return f"Region:{region} Bucket:{bucket_name} Key:{key}"

    def __init__(self, region: str = None, bucket: str = None):
        assert region
        assert bucket
        self.bucket = bucket
        self.region = region

    def fetch(self, datastream_name: str, start_time: str, end_time: str, local_path: str = None) -> List[str]:
        pass

    def save(self, local_paths: List[str]) -> None:
        pass

    def exists(self, datastream_name: str, start_time: str, end_time: str) -> bool:
        pass

    def delete(self, datastream_name: str, start_time: str, end_time: str) -> None:
        pass
