import os
import random
import unittest
from multiprocessing.managers import SyncManager
from typing import List
from unittest.mock import patch

from typing.io import IO, TextIO

from src.ingestion.warc import WarcIngestor, WarcFile


class MockHeaders:
    def __init__(self, uri: str):
        self.uri = uri

    def get_header(self, header: str):
        if header == 'WARC-Target-URI':
            return self.uri
        return None


class MockContentStream:
    def __init__(self, content: str):
        self.content = content

    def read(self) -> str:
        return self.content


class MockRecord:
    def __init__(self, uri: str, content: str, rec_type: str = 'response'):
        self.rec_headers = MockHeaders(uri)
        self.content = MockContentStream(content)
        self.rec_type = rec_type

    def content_stream(self):
        return self.content


class MockArchiveIterator:
    def __init__(self, records: List[MockRecord] = []):
        self.records = records[:]

    def __next__(self):
        if len(self.records) == 0:
            raise StopIteration
        return self.records.pop(0)

    def close(self):
        pass


class FakeIndexFile(TextIO):
    def __init__(self, num_entries: int):
        self.num_entries = num_entries
        self.curr_read = 0

    def readline(self):
        if self.curr_read == self.num_entries:
            return ''
        year = str(random.choice(range(2015, 2022)))
        month = add_leading_zero(str(random.choice(range(0, 12)) + 1))
        day = add_leading_zero(str(random.choice(range(0, 30)) + 1))
        hour = add_leading_zero(str(random.choice(range(0, 24))))
        minute = add_leading_zero(str(random.choice(range(0, 60))))
        second = add_leading_zero(str(random.choice(range(0, 60))))
        self.curr_read += 1
        return f'{year}{month}{day}{hour}{minute}{second}'

    def close(self):
        pass


class MockS3Connector:
    def get_bucket(self, bucket: str):
        return bucket


def mock_s3_connector_fn(aws_access_key_id: str, aws_secret_access_key: str):
    return MockS3Connector()


def mock_basic_archive_iterator_fn():
    def _mock_archive_iterator_fn(fp: IO):
        records = [
            MockRecord('http://foo.com', '{"first": 1}'),
            MockRecord('http://bar.com', '{"second": 2}'),
            MockRecord('http://baz.com', '{"third": 3}')
        ]
        return MockArchiveIterator(records)

    return _mock_archive_iterator_fn


def mock_archive_iterator_with_non_response_types_fn():
    def _mock_archive_iterator_fn(fp: IO):
        records = [
            MockRecord('http://foo.com', '{"first": 1}'),
            MockRecord('http://bar.com', '{"second": 2}', 'random'),
            MockRecord('http://baz.com', '{"third": 3}')
        ]
        return MockArchiveIterator(records)

    return _mock_archive_iterator_fn


def add_leading_zero(_str: str) -> str:
    if len(_str) == 1:
        return '0' + _str
    return _str


class TestWarcIngestor(WarcIngestor):
    def __init__(self, num_index_entries=1, archive_iterator_fn=mock_basic_archive_iterator_fn):
        os.environ['AWS_ACCESS_KEY_ID'] = 'foo'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'bar'
        self.index_calls = [i for i in range(num_index_entries)]
        manager = SyncManager()
        manager.start()
        super().__init__('foo', FakeIndexFile(num_index_entries),
                         archive_iterator_fn=archive_iterator_fn(), keep_local_files=True, manager=manager)

    def _get_local_warc_file(self):
        if len(self.index_calls) > 0:
            self.index_calls.pop()
            return WarcFile("foo", 1, 1), None, None
        raise StopIteration()


class BasicWarcTests(unittest.TestCase):
    @patch('src.ingestion.warc.download_warc_files')
    def test_single_file_index(self, mock_download_warc_files):
        mock_download_warc_files.return_value = None
        ingestor = TestWarcIngestor()
        self.assertTrue(ingestor.next().content == '{"first": 1}')
        self.assertTrue(ingestor.next().content == '{"second": 2}')
        self.assertTrue(ingestor.next().content == '{"third": 3}')
        with self.assertRaises(StopIteration):
            ingestor.next()

    @patch('src.ingestion.warc.download_warc_files')
    def test_non_response_type(self, mock_download_warc_files):
        mock_download_warc_files.return_value = None
        ingestor = TestWarcIngestor(num_index_entries=1,
                                    archive_iterator_fn=mock_archive_iterator_with_non_response_types_fn)
        self.assertTrue(ingestor.next().content == '{"first": 1}')
        self.assertTrue(ingestor.next().content == '{"third": 3}')
        with self.assertRaises(StopIteration):
            ingestor.next()

    @patch('src.ingestion.warc.download_warc_files')
    def test_multiple_file_index(self, mock_download_warc_files):
        mock_download_warc_files.return_value = None
        num_entries = 24
        ingestor = TestWarcIngestor(num_entries)
        for i in range(num_entries):
            self.assertTrue(ingestor.next().content == '{"first": 1}')
            self.assertTrue(ingestor.next().content == '{"second": 2}')
            self.assertTrue(ingestor.next().content == '{"third": 3}')
        with self.assertRaises(StopIteration):
            ingestor.next()
