import threading
from typing import Dict, List

from newspaper import Article

from src.processors.processor import Processor
from src.processors.types import Record


class NewsProcessor(Processor):
    def __init__(self, results: List[Dict], mutex: threading.Lock):
        self.results = results
        self.mutex = mutex
        super().__init__()

    def process(self, record: Record):
        locked = False
        try:
            article = Article('')
            article.download(input_html=record.content)
            article.parse()
            if not article.is_parsed:
                self.results.append({'error': f'Error processing record: Could not parse'})
            self.mutex.acquire()
            locked = True
            if article.meta_lang == 'en':
                self.results.append(
                    {
                        "uri": record.uri,
                        "ts": record.ts,
                        "title": article.title,
                        "text": article.text
                    }
                )
        except Exception as e:
            self.results.append({'error': f'Error processing record: {e}'})
        finally:
            if locked:
                self.mutex.release()


