import threading
from typing import Dict, List

from src.processors.types import Record


class Processor:
    def _init__(self, results: List[Dict], mutex: threading.Lock):
        self.mutex = mutex
        self.results = results

    def process(self, record: Record):
        pass

