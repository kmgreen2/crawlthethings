import re
import uuid

from src.storage.s3 import S3Object, get_s3_credentials


class StorageDescriptor:
    def __init__(self, output_path: str):
        self.output_path = output_path
        self.s3_pattern = 's3://([a-zA-Z0-9\\-]+)\\.([a-zA-Z0-9\\-]+)/(.*)'
        type_re = re.compile('(s3|file)://.*')
        match_type = type_re.match(output_path)
        if match_type:
            self.file_type = match_type.group(1)
            if self.file_type == 's3':
                self.aws_access_key_id, self.aws_secret_access_key = get_s3_credentials()
                path_re = re.compile(self.s3_pattern)
                match = path_re.match(output_path)
                if match:
                    self.region = match.group(1)
                    self.bucket = match.group(2)
                    self.path = match.group(3)
                else:
                    raise Exception(f'Bad S3 specification: {self.output_path}.  Expected: "{self.s3_pattern}"')

            elif self.file_type == 'file':
                path_re = re.compile('file://(.*)')
                match = path_re.match(output_path)
                if match:
                    self.path = match.group(1)
            else:
                raise Exception(f'Unknown file type: {self.file_type}')


class StorageObject:
    def __init__(self, desc: StorageDescriptor):
        self.desc = desc
        if self.desc.file_type == 's3':
            self.local_filename = f'/tmp/{str(uuid.uuid4())}{desc.bucket}-{desc.path.replace("/", ":")}'
            self.append_file = open(self.local_filename, "a+")
        elif self.desc.file_type == 'file':
            self.append_file = open(desc.path, "a+")
        else:
           raise Exception(f'Unknown file type: {self.desc.file_type}')

    def append(self, payload: str) -> int:
        return self.append_file.write(payload)

    def close_and_flush(self):
        self.append_file.close()
        if self.desc.file_type == 's3':
            print(f'Flushing {self.local_filename} to {self.desc.file_type}://{self.desc.region}.{self.desc.bucket}/{self.desc.path}')
            remote_object = S3Object(self.desc.bucket, self.desc.region, self.desc.path, self.desc.aws_access_key_id,
                                     self.desc.aws_secret_access_key)
            remote_object.put(self.local_filename)

