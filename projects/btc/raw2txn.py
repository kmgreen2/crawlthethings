import argparse
import base64
import gzip
import json
import re
from typing import Tuple, Any, Iterable, Dict, Set, List, Callable

from typing.io import TextIO

from src.util.logging import Logger

logger = Logger()


def get_file_type_and_path(path: str) -> Tuple[str, str]:
    type_re = re.compile('(s3|file)://(.*)')
    match_type = type_re.match(path)
    if match_type:
        file_type = match_type.group(1)
        path = match_type.group(2)
        return file_type, path
    return None


class RawInput(Iterable):
    def iter(self):
        return self

    def __iter__(self):
        return self.iter()

    def __init__(self, input_file):
        self.input_file = input_file
        file_type, path = get_file_type_and_path(input_file)
        if file_type == 's3':
            # Todo: implement
            pass
        elif file_type == 'file':
            self.fd = open(path, 'r')
        else:
            raise Exception('Invalid input file format')

    def __next__(self) -> Any:
        line = self.fd.readline()
        if line == '':
            raise StopIteration()
        decoded_line = base64.b64decode(bytes(line, 'utf-8'))
        decompressed_line = gzip.decompress(decoded_line).decode('utf-8')
        payload = json.loads(decompressed_line)
        return json.loads(payload['content'])


class Output:
    def __init__(self, output_file):
        self.output_file = output_file
        self.fd = open(self.output_file, 'w')

    def append(self, block: int, idx: int, group_addr: str, out_addr: str, amount: float, fee: float):
        self.fd.write(f'{block} {idx} {group_addr} {out_addr} {amount} {fee}\n')

    def footer(self, writer: Callable[[TextIO], None]):
        self.fd.write('FOOTER\n')
        writer(self.fd)

    def close(self):
        self.fd.close()


# We assume all in addresses are owned by the same entity, so organize them
# as an address group
class AddressGroup:
    def __init__(self):
        self.addrs: Dict[str, Set] = {}
        self.reverse: Dict[str, str] = {}

    def add(self, addrs=[]):
        found = False
        for addr in addrs:
            if addr in self.reverse:
                group_addr = self.reverse[addr]
                found = True
                self.addrs[group_addr] = self.addrs[group_addr].union(set(addrs))
                break
        if found is False:
            self.addrs[addrs[0]] = set(addrs)
            group_addr = addrs[0]
        for addr in addrs:
            self.reverse[addr] = group_addr

    def group_addr(self, addr):
        if addr in self.reverse:
            return self.reverse[addr]
        self.reverse[addr] = addr
        self.addrs[addr] = set([addr])
        return addr

    def write(self, fd: TextIO):
        for k in self.reverse:
            fd.write(f'{k} {self.reverse[k]}\n')


def parse():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-i', '--input', help='Input path (e.g. s3://<bucket>/<path> or file://<path>) containing a '
                                              'compressed, b64 encoded BTC block per line',
                        required=True)
    parser.add_argument('-o', '--output', help='Output path (e.g. s3://<bucket>/<path> or file://<path>)',
                        required=False)
    return parser.parse_args()


def process_inputs(inputs: List[Dict]) -> Tuple[List[str], float]:
    addrs = []
    total_value = 0.0
    for i in inputs:
        value = i['prev_out']['value']
        addr = None
        if 'addr' in i['prev_out']:
            addr = i['prev_out']['addr']
        if value == 0 and addr is None:
            if len(inputs) > 1:
                raise Exception('Unexpected COINBASE')
            return ['COINBASE'], 0.0
        elif addr is None:
            # There are cases where value > 0 and addr is None, specifically OP_RETURN
            # Just skip as the input is not spendable
            continue
        else:
            addrs.append(addr)
            total_value += value
    return addrs, total_value


def process_outputs(outputs: List[Dict]) -> List[Tuple[str, float]]:
    results = []
    for o in outputs:
        if o['value'] > 0 and 'addr' in o:
            value = o['value']
            addr = o['addr']
            results.append((addr, value))
    return results


def main():
    args = parse()
    raw_input = RawInput(args.input)
    output = Output(args.output)
    addr_group = AddressGroup()

    for block_input in raw_input:
        i = 0
        for block in block_input['blocks']:
            block_index = block['block_index']
            for txn in block['tx']:
                fee = txn['fee']
                addrs, value = process_inputs(txn['inputs'])
                # Skip where there are no valid input addresses (e.g. OP_RETURN)
                if len(addrs) == 0:
                    continue
                addr_group.add(addrs)
                group_addr = addr_group.group_addr(addrs[0])
                results = process_outputs(txn['out'])
                if results is not None and group_addr is not None and fee is not None:
                    for result in results:
                        output.append(block_index, i, group_addr, result[0], result[1], fee)
                i += 1
    output.footer(addr_group.write)
    output.close()


if __name__ == '__main__':
    main()
