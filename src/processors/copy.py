import threading
from typing import List, Dict

from src.processors.processor import Processor
from src.processors.types import Record


class CopyProcessor(Processor):
    def __init__(self, results: List[Dict], mutex: threading.Lock):
        self.results = results
        self.mutex = mutex
        super().__init__()

    def process(self, record: Record):
        try:
            self.mutex.acquire()
            self.results.append(
                {
                    "uri": record.uri,
                    "ts": record.ts,
                    "content": record.content
                }
            )
        except Exception as e:
            self.results.append({'error': f'Error processing record: {e}'})
        finally:
            self.mutex.release()

