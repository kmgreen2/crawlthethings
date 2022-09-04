import os
import re
import shutil
import threading
import time
import uuid
from datetime import datetime
from multiprocessing.managers import SyncManager
from multiprocessing.pool import ThreadPool
from typing import Tuple, List

import boto
from boto.s3.key import Key
from typing.io import BinaryIO, IO, TextIO
from warcio import ArchiveIterator
from warcio.checker import Checker
from warcio.exceptions import ArchiveLoadFailed
from warcio.recompressor import Recompressor

from src.ingestion.ingestor import Ingestor
from src.processors.types import Record
from src.storage.s3 import get_s3_credentials
from src.util.logging import Logger
from src.util.s3helpers import get_warc_s3_key


class WarcCheckerArgs:
    def __init__(self, inputs=None, verbose=False):
        self.inputs = inputs
        self.verbose = verbose


class WarcFile:
    def __init__(self, key: str, offset: int, length: int = -1):
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
        return time.mktime(
            datetime.strptime(f'{year}-{month}-{day} {hour}:{minute}:{second}', '%Y-%m-%d %H:%M:%S').timetuple())


def warc_file_from_line(line: str) -> WarcFile:
    line_ary = line.strip().split(" ")

    if len(line_ary) == 3:
        return WarcFile(line_ary[0], int(line_ary[1]), int(line_ary[1]) + int(line_ary[2]))
    elif len(line_ary) == 2:
        return WarcFile(line_ary[0], int(line_ary[1]), -1)
    elif len(line_ary) == 1:
        return WarcFile(line_ary[0], 0, -1)
    else:
        raise Exception(f'Malformed input line: {line}')


def default_s3_connector(aws_access_key_id: str, aws_secret_access_key: str):
    return boto.connect_s3(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)


def default_archive_iterator(fp: IO):
    return ArchiveIterator(fp)


class WarcCacheEntry:
    def __init__(self, warc_file: WarcFile, warc_fp: BinaryIO, local_path: str):
        self.warc_file = warc_file
        self.warc_fp = warc_fp
        self.local_path = local_path


def download_warc_file(k: Key, path: str):
    with open(path, 'wb') as fp:
        k.get_file(fp)


def check_warc_file(path: str) -> bool:
    checker = Checker(WarcCheckerArgs())
    try:
        checker.process_one(path)
    except:
        return False
    return checker.exit_value == 0


def recompress_warc_file(from_path: str, to_path: str):
    recompressor = Recompressor(from_path, to_path)
    recompressor.recompress()


def get_local_warc_file(key: str, offset: int, length: int, bucket: str) -> Tuple[BinaryIO, str]:
    aws_access_key_id, aws_secret_access_key = get_s3_credentials()
    conn = default_s3_connector(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    bucket_conn = conn.get_bucket(bucket)
    local_filename = str(uuid.uuid4())
    local_path = f'/tmp/{local_filename}'
    k = get_warc_s3_key(key, offset, length, bucket=bucket_conn)
    download_warc_file(k, local_path)
    if not check_warc_file(local_path):
        recompress_warc_file(local_path, f'{local_path}.tmp')
        shutil.move(f'{local_path}.tmp', local_path)
    return open(local_path, 'rb'), local_path


def download_warc_files(bucket: str, index_fp: TextIO, cache: List[WarcCacheEntry], max_cache_len: int,
                        files_downloading: threading.Condition):
    while True:
        warc_files = []
        eof = False

        # If we have room in the cache, get the index lines and derive the S3 keys
        while len(cache) < max_cache_len:
            line = index_fp.readline()
            if line == '':
                eof = True
                break
            warc_file = warc_file_from_line(line)
            warc_files.append(warc_file)

        # If we have keys to process, download the files, otherwise, go to sleep
        if len(warc_files) > 0:
            pool = ThreadPool(len(warc_files))
            results = pool.starmap(get_local_warc_file,
                                   map(lambda x: (x.key, x.offset, x.length, bucket), warc_files))
            zipped_results = zip(warc_files, results)
            pool.close()
            pool.join()
            for result in zipped_results:
                cache.append(WarcCacheEntry(result[0], result[1][0], result[1][1]))
            with files_downloading:
                files_downloading.notify()
        elif not eof:
            time.sleep(10)

        if eof:
            index_fp.close()
            with files_downloading:
                files_downloading.notify()
            break


class WarcIngestor(Ingestor):
    def __init__(self, bucket: str, input_fp: TextIO, archive_iterator_fn=default_archive_iterator,
                 manager: SyncManager = SyncManager(),
                 keep_local_files=False):
        self.logger = Logger()
        self.bucket = bucket
        self.index_fp = input_fp
        self.archive_iterator_fn = archive_iterator_fn
        self.curr_ts = None
        self.archive_iterator = None
        self.warc_fp = None
        self.warc_local_path = None
        self.keep_local_files = keep_local_files

        self.manager = manager
        self.files_downloading = self.manager.Condition()
        self.warc_file_cache: List[WarcCacheEntry] = []

        self.download_thread = threading.Thread(target=download_warc_files,
                                                args=(self.bucket, self.index_fp, self.warc_file_cache, 4,
                                                      self.files_downloading))
        self.download_thread.start()

    def _get_local_warc_file(self):
        while len(self.warc_file_cache) == 0 and not self.index_fp.closed:
            with self.files_downloading:
                self.files_downloading.wait(timeout=5)
        if len(self.warc_file_cache) > 0:
            entry = self.warc_file_cache.pop(0)
            return entry.warc_file, entry.warc_fp, entry.local_path
        else:
            raise StopIteration

    def _get_next_index_line(self):
        if self.archive_iterator is not None and self.warc_fp is not None:
            self.logger.warning(f'Closing old archive')
            self.archive_iterator.close()
            # close local file
            self.warc_fp.close()
            # delete local file
            if self.keep_local_files is False:
                os.remove(self.warc_local_path)
        warc_file, self.warc_fp, self.warc_local_path = self._get_local_warc_file()
        self.archive_iterator = self.archive_iterator_fn(self.warc_fp)
        self.curr_ts = warc_file.timestamp()

    def next(self) -> Record:
        parsed_record = None
        # This is needed to init
        if self.archive_iterator is None:
            self._get_next_index_line()
        while parsed_record is None:
            try:
                record = self.archive_iterator.__next__()
                if record.rec_type == 'response':
                    parsed_record = Record(record.rec_headers.get_header('WARC-Target-URI'), self.curr_ts,
                                           record.content_stream().read())
            except ArchiveLoadFailed as e:
                # ToDo(KMG): Should we mark or log this?
                self.logger.warning(f'Archive load failed: {str(e)}')
                continue
            except StopIteration:
                # This means we hit the current EOF, so must fetch the next index file
                self.logger.warning(f'Hit EOF')
                try:
                    self._get_next_index_line()
                except StopIteration as e:
                    self.download_thread.join()
                    raise e
            except Exception as e:
                self.logger.error(f'Unhandled exception: {str(e)}')
                raise e

        return parsed_record
