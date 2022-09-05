import argparse
import base64
import gzip
import json
import re


def parse():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-i', '--input',
                        help='Input file containing entries processed by the main crawlthethings processor',
                        required=True)
    return parser.parse_args()


def main(input_file: str):
    distribution = {}
    with open(input_file) as f:
        while True:
            line = f.readline()
            if line != '':
                decoded = base64.b64decode(line)
                decompressed = gzip.decompress(decoded)
                payload = json.loads(decompressed)
                if 'uri' in payload:
                    result = re.search(r'https?:\/\/([^\ /]*)', payload['uri'])
                    if result is not None and result.group(1):
                        domain = result.group(1)
                        if domain not in distribution:
                            distribution[domain] = 0
                        distribution[domain] += 1
                    else:
                        raise Exception(f"Could not extract domain from {payload['uri']}")
            else:
                break
    sorted = []
    for k in distribution:
        sorted.append((k, distribution[k]))
    sorted.sort(key=lambda x: x[1], reverse=True)

    for elm in sorted:
        print(f'{elm[0]}: {elm[1]}')


if __name__ == '__main__':
    args = parse()
    main(args.input)
