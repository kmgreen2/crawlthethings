import logging

import boto
import os
import sys
from boto.s3.key import Key
from boto.s3.connection import S3Connection
from typing import Tuple

from src.storage.remote import RemoteObject


def get_s3_credentials() -> Tuple[str, str]:
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    if aws_access_key_id is None or aws_secret_access_key is None:
        raise Exception('Must set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY when uploading results to S3')
    return aws_access_key_id, aws_secret_access_key


class S3Object(RemoteObject):
    def __init__(self, bucket: str, region: str, path: str, aws_access_key_id: str, aws_secret_access_key: str):
        self.bucket = bucket
        self.region = region
        self.path = path
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        conn = S3Connection(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key,
                            host=f's3.{self.region}.amazonaws.com')
        bucket_conn = conn.get_bucket(bucket)
        self.k = Key(bucket_conn, path)

    def put(self, local_path: str) -> int:
        written = 0
        with open(local_path, 'r') as fd:
            try:
                position = fd.tell()
                fd.seek(0, os.SEEK_END)
                self.k.size = fd.tell()
                fd.seek(position)
                self.k.send_file(fd)
            except Exception as e:
                logging.Logger.info(e)
            written = self.k.size
            self.k.close()
        return written
