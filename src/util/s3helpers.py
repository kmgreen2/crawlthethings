from boto.s3.bucket import Bucket
from boto.s3.key import Key


# Wrapper around S3 key for situations where we have the offset, but no length.  In this case, we
# cannot set range headers, so internally seek when opening the key
class KeyAtOffset(Key):
    def __init__(self, key: str, seek_to_offset: int, bucket: Bucket = None, name: str = None):
        super().__init__(bucket, name)
        self.offset = 0
        self.chunk_size = 1024 * 1024 * 64
        self.key = key
        self._seek(seek_to_offset)

    def tell(self) -> int:
        return self.offset

    def _seek(self, seek_to_offset: int):
        remaining = seek_to_offset
        while remaining > 0:
            chunk_size = self.chunk_size
            if self.chunk_size > remaining:
                chunk_size = remaining
            result = self.read(chunk_size)
            remaining -= len(result)
        self.offset = seek_to_offset


# Wrapper around S3 key for situations where we have the offset and length.  Here, we set the range headers
class KeyChunk(Key):
    def __init__(self, key: str, start: int, end: int, bucket: Bucket = None, name: str = None):
        super().__init__(bucket, name)
        self.offset = start
        self.start = start
        self.end = end
        self.key = key
        self.open_read(headers={'Range': f'bytes={start}-{end}'})

    def tell(self) -> int:
        return self.offset

    def read(self, size=-1):
        data = super().read(size)
        self.offset += len(data)
        return data


def get_warc_s3_key(key: str, start: int, end: int, bucket: Bucket = None, name: str = None) -> Key:
    if end > -1:
        return KeyChunk(key, start, end, bucket, name)
    elif start > 0:
        return KeyAtOffset(key, start, bucket, name)
    else:
        k = Key(bucket, name)
        k.key = key
        return k
