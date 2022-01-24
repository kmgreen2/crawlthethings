import csv
from src.ingestion.ingestor import Ingestor

from src.processors.types import Record


class CSVIngestor(Ingestor):
    def __init__(self, csvfile: str) -> Record:
        csvfile_fd = open(csvfile, newline='')
        self.csvreader = csv.DictReader(csvfile_fd)

    def next(self) -> Record:
        entry = self.csvreader.__next__()
        return Record(entry['uri'], entry['ts'], entry)