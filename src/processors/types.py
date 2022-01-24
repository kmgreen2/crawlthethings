class Record:
    def __init__(self, uri, ts, content):
        self.uri = uri
        self.ts = ts
        if isinstance(content, (bytes, bytearray)):
            self.content = content.decode('utf-8', 'ignore')
        else:
            self.content = content


