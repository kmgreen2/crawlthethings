import multiprocessing
import threading
from typing import List, Dict, Iterator

import json
import sys
import argparse
from multiprocessing import Pool, Manager
from multiprocessing.managers import SyncManager

from src.ingestion.csv import CSVIngestor
from src.ingestion.warc import WarcIngestor
from src.processors.copy import CopyProcessor
from src.processors.news import NewsProcessor
from src.processors.rottentomatoes import RottenTomatoesProcessor
from src.processors.types import Record
from src.storage.storage import StorageObject, StorageDescriptor
from src.util.logging import Logger

logger = Logger()

def parse():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-i','--input', help='Input file containing ingest-specific configuration', required=True)
    parser.add_argument('-o','--output', help='Output path (e.g. s3://<bucket>/<path> or file://<path>)', required=False)
    parser.add_argument('-p','--processor', help='Processor to use (e.g. news)', required=True)
    parser.add_argument('-I','--ingestor', help='Ingestor to use (e.g. warc-index)', required=True)
    parser.add_argument('-t','--threads', help='Number of threads (default=16)', default=16)
    return parser.parse_args()


def flush_results(storage_object: StorageObject, results: List[Dict]):
    if len(results) > 0:
        logger.warning(f'Appending {len(results)} results')
        json_str = json.dumps(list(results))
        storage_object.append(json_str)

        del results[:]


def do_process(processor: str, storage_object: StorageObject, record: Record, results: List[Dict], mutex: threading.Lock, semaphore: threading.Semaphore):
    try:
        if processor == 'news':
            NewsProcessor(results, mutex).process(record)
        elif processor == 'copy':
            CopyProcessor(results, mutex).process(record)
        elif processor == 'rottentomatoes':
            RottenTomatoesProcessor(results, mutex).process(record)
        else:
            raise Exception(f'Unknown processor: {processor}')

        if len(results) > 0 and len(results) % 100 == 0:
            with mutex:
                flush_results(storage_object, results)
    except Exception as e:
        logger.error(str(e))
    finally:
        semaphore.release()


def error_callback(e: Exception):
    logger.error(f'Error running process: {str(e)}')


def callback(value):
    pass


def main():
    args = parse()
    storage_desc = StorageDescriptor(args.output)
    SyncManager.register('StorageObject', StorageObject)
    with SyncManager() as manager:
        with Pool(int(args.threads)) as p:
            results = manager.list([])
            mutex = manager.Lock()
            semaphore = manager.Semaphore(int(args.threads))
            storage_object = manager.StorageObject(storage_desc)
            if args.ingestor == 'warc-index':
                ingestor = WarcIngestor('commoncrawl', args.input)
            elif args.ingestor == 'csv-file':
                ingestor = CSVIngestor(args.input)
            else:
                raise Exception(f'Unknown ingestor: {args.ingestor}')

            for record in ingestor:
                semaphore.acquire()
                p.apply_async(do_process, (args.processor, storage_object, record, results, mutex, semaphore),
                              callback=callback, error_callback=error_callback)
            p.close()
            p.join()

            with mutex:
                flush_results(storage_object, results)
                try:
                    storage_object.close_and_flush()
                except Exception as e:
                    logger.error(f'Error closing storage object: {str(e)}')
                    raise e


if __name__ == '__main__':
    main()


