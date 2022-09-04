import datetime

import csv
from src.ingestion.ingestor import Ingestor

from src.processors.types import Record
from src.util.logging import Logger

import requests
import time

logger = Logger()

class BTCIngestor(Ingestor):
    def __init__(self, input_str: str) -> Record:
        input_ary = input_str.split(",")
        self.base_url = input_ary[0]
        self.begin = int(input_ary[1])
        self.end = int(input_ary[2])
        self.current = self.begin

    def next(self) -> Record:
        url = f'{self.base_url}/{self.current}'
        retry_num = 0
        while retry_num < 3:
            retry_num += 1
            try:
                if self.current <= self.end:
                    result = requests.get(f'{self.base_url}/{self.current}', params={format: 'json'})
                    result.encoding = result.apparent_encoding
                    self.current += 1
                    return Record(url, time.time(), result.text.encode("utf-8"))
                else:
                    raise StopIteration()

            except ConnectionResetError as e:
                logger.warning(f'Retrying ({retry_num}) after ConnectionResetError: {str(e)}')
                time.sleep(2 ** (retry_num-1))
                continue
            except requests.exceptions.ChunkedEncodingError as e:
                logger.warning(f'Retrying ({retry_num}) after ChunkedEncodingError: {str(e)}')
                time.sleep(2 ** (retry_num-1))
                continue
            except StopIteration as e:
                raise e
            except Exception as e:
                logger.error(f'Stopping prematurely: {str(e)}')
                raise StopIteration()
