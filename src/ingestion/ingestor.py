from collections import Iterable

from src.processors.types import Record


class Ingestor(Iterable):
    def __next__(self) -> Record:
        return self.next()

    def __iter__(self):
        return self.iter()

    def next(self) -> Record:
        pass

    def iter(self):
        return self;
