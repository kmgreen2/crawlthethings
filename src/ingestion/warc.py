import os
import shutil
import uuid
from typing import Tuple

from boto.file import Key
from typing.io import BinaryIO
from warcio import ArchiveIterator
from warcio.checker import Checker
from warcio.exceptions import ArchiveLoadFailed
from warcio.recompressor import Recompressor

from src.ingestion.ingestor import Ingestor
from datetime import datetime

from src.storage.s3 import get_s3_credentials
from src.util.logging import Logger
from src.util.s3helpers import get_warc_s3_key
import boto
import time
import re

from src.processors.types import Record


class WarcCheckerArgs:
    def __init__(self, inputs=None, verbose=False):
        self.inputs = inputs
        self.verbose = verbose


class WarcFile:
    def __init__(self, key: str, offset: int, length:int=-1):
        self.key = key
        self.offset = offset
        self.length = length

    # This will derive a timestamp from the WARC file
    def timestamp(self) -> float:
        dt_formatted = None
        warc_key_re = re.search('([0-9]{14})\-[0-9]{5}', self.key)
        if warc_key_re is not None:
            dt_formatted = warc_key_re.group(1)
        else:
            timestamp_re = re.search('[0-9]{4}/[0-9]{2}/[0-9]{2}/[0-9]+/([0-9]+)_[0-9]+', self.key)
            if timestamp_re is not None:
                ts_str = timestamp_re.group(1)
                dt = datetime.fromtimestamp(float(ts_str) / 1000, tz=None)
                dt_formatted = dt.strftime('%Y%m%d%H%M%S')
            else:
                return 0.0

        year = dt_formatted[:4]
        month = dt_formatted[4:6]
        day = dt_formatted[6:8]
        hour = dt_formatted[8:10]
        minute = dt_formatted[10:12]
        second = dt_formatted[12:14]
        return time.mktime(datetime.strptime(f'{year}-{month}-{day} {hour}:{minute}:{second}', '%Y-%m-%d %H:%M:%S').timetuple())


def warc_file_from_line(line: str) -> WarcFile:
    line_ary = line.strip().split(" ")

    if len(line_ary) == 3:
        return WarcFile(line_ary[0], int(line_ary[1]),  int(line_ary[1])+int(line_ary[2]))
    elif len(line_ary) == 2:
        return WarcFile(line_ary[0], int(line_ary[1]), -1)
    elif len(line_ary) == 1:
        return WarcFile(line_ary[0], 0, -1)
    else:
        raise Exception(f'Malformed input line: {line}')


class WarcIngestor(Ingestor):
    def __init__(self, bucket: str, input_index_file: str):
        self.logger = Logger()
        self.bucket = bucket
        self.input_index_file = input_index_file
        aws_access_key_id, aws_secret_access_key = get_s3_credentials()
        conn = boto.connect_s3(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        self.bucket_conn = conn.get_bucket(self.bucket)
        self.index_fp = open(input_index_file)
        self.curr_ts = None
        self.archive_iterator = None
        self.warc_fp = None
        self.warc_local_path = None
        self._get_next_index_line()

    def _download_warc_file(self, k: Key, path: str):
        with open(path, 'wb') as fp:
            k.get_file(fp)

    def _check_warc_file(self, path: str) -> bool:
        checker = Checker(WarcCheckerArgs())
        try:
            checker.process_one(path)
        except:
            return False
        return checker.exit_value == 0

    def _recompress_warc_file(self, from_path: str, to_path: str):
        recompressor = Recompressor(from_path, to_path)
        recompressor.recompress()

    def _get_local_warc_file(self, k: Key) -> Tuple[BinaryIO, str]:
        local_filename = str(uuid.uuid4())
        local_path = f'/tmp/{local_filename}'
        self.logger.warning(f'Downloading {k.name} to {local_path}')
        self._download_warc_file(k, local_path)
        self.logger.warning(f'Checking {local_path}')
        if not self._check_warc_file(local_path):
            self.logger.warning(f'Checking {local_path} failed, recompressing')
            self._recompress_warc_file(local_path, f'{local_path}.tmp')
            shutil.move(f'{local_path}.tmp', local_path)
        self.logger.warning(f'Opening {local_path}')
        return open(local_path, 'rb'), local_path

    def _get_next_index_line(self):
        line = self.index_fp.readline()
        if line == '':
            self.logger.warning('Hit end of index')
            raise StopIteration()
        self.logger.warning(f'Fetching index entry: {line}')
        warc_file = warc_file_from_line(line)
        k = get_warc_s3_key(warc_file.key, warc_file.offset, warc_file.length, bucket=self.bucket_conn)
        self.logger.warning(f'Fetched index entry: {line}')
        if self.archive_iterator is not None and self.warc_fp is not None:
            self.logger.warning(f'Closing old archive')
            self.archive_iterator.close()
            # close local file
            self.warc_fp.close()
            # delete local file
            os.remove(self.warc_local_path)
        self.warc_fp, self.warc_local_path = self._get_local_warc_file(k)
        self.archive_iterator = ArchiveIterator(self.warc_fp)
        self.curr_ts = warc_file.timestamp()

    def next(self) -> Record:
        parsed_record = None
        stopped_raised = False

        while parsed_record is None:
            try:
                record = self.archive_iterator.__next__()
                if record.rec_type == 'response':
                    parsed_record = Record(record.rec_headers.get_header('WARC-Target-URI'), self.curr_ts, record.content_stream().read())
            except ArchiveLoadFailed as e:
                # ToDo(KMG): Should we mark or log this?
                self.logger.warning(f'Archive load failed: {str(e)}')
                continue
            except StopIteration as e:
                # This means we hit the current EOF, so must fetch the next index file
                self.logger.warning(f'Hit EOF: {str(e)}')
                stopped_raised = True
                break
            except Exception as e:
                self.logger.error(f'Unhandled exception: {str(e)}')
                raise e

        # Keeping the call outside of the exception handler, as this can also raise StopIteration
        if stopped_raised is True:
            self._get_next_index_line()
            stopped_raised = False

        return parsed_record
