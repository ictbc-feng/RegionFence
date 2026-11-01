"""The nine mainnet traces, named oldest to newest, and the loaders shared by the tools."""
import gzip
import json
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, "data")
RESULTS = os.path.join(ROOT, "results")

TRACE_LENGTH = 100
TRACES = {
    "A": 12499900,
    "B": 13809900,
    "C": 14500000,
    "D": 14999900,
    "E": 16619900,
    "F": 17873600,
    "G": 20079900,
    "H": 22449900,
    "I": 26060700,
}


def trace_blocks(trace):
    return range(TRACES[trace], TRACES[trace] + TRACE_LENGTH)


def all_trace_blocks():
    return [number for trace in TRACES for number in trace_blocks(trace)]


def trace_of(number):
    """The trace holding a block, or None for a block outside all of them."""
    for trace, first in TRACES.items():
        if first <= number < first + TRACE_LENGTH:
            return trace
    return None


def read_gzip_json(path):
    with gzip.open(path, "rt") as source:
        return json.load(source)


def read_block(kind, number, data=DATA):
    """One block's record from data/rwsets or data/screen."""
    return read_gzip_json(os.path.join(data, kind, f"{number}.json.gz"))


def read_rows(path):
    """The rows of a JSONL file written by rfreplay."""
    with open(path) as source:
        return [json.loads(line) for line in source if line.strip()]
