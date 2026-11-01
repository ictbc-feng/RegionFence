#!/usr/bin/env python3
"""Turn results/ and data/ into the CSV files behind the figures, in figures/data/.

  a          Figure A from results/synth
  b          Figure B from data/screen and data/rwsets
  c          Figure C from results/scaling, results/positions, data/positions and data/rwsets
  d          Figure D(b) from results/scaling; D(a) reads the curves of Figure C
  mechanism  Figures E, F and G(b, c) from results/mechanism and data/rwsets
  control    Figure G(a) from results/control

Abort rate is aborts / exec, speedup is over the system's own single core.
"""
import argparse
import collections
import csv
import heapq
import json
import os
import statistics
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))
from traces import DATA, RESULTS, ROOT, TRACES, read_block, read_rows, trace_blocks, trace_of

CORES = (1, 2, 4, 8, 16, 32, 64, 128)
FIXED_CORES = 64
SYSTEMS = {"regionfence": "RegionFence", "occ": "OCC", "pcc": "PCC"}
CONTROLS = ("default", "no_cross_block", "no_learned_waits")
SAMPLE = range(12000000, 26060001, 10000)
SAMPLE_MINIMUM_TRANSACTIONS = 10
SLIDING_BLOCKS = 16
MINIMUM_FENCE_EVENTS = 20
GANTT_TRANSACTIONS = 7
STORAGE_KIND = "3"


def write_csv(options, name, header, rows):
    with open(os.path.join(options.out, f"{name}.csv"), "w", newline="") as sink:
        writer = csv.writer(sink, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)


def measurement(path, cores):
    """Wall seconds, abort rate of the single row of a one-window item."""
    row = read_rows(path)[0]
    return row[f"p{cores}_ns"] / 1e9, row[f"p{cores}_aborts"] / row[f"p{cores}_exec"]


def measured_row(path_of, system, cores):
    """Wall, speedup over the single core and abort rate of one configuration."""
    wall, abort_rate = measurement(path_of(system, cores), cores)
    return [round(wall, 4), round(measurement(path_of(system, 1), 1)[0] / wall, 3), round(abort_rate, 4)]


# Figure A

def synth_panels():
    """(panel, x, scenario, cores) of every point; the default point is shared across the sweeps."""
    for theta in ("0", "0.4", "0.6", "0.8", "0.9", "0.99", "1.2"):
        yield "a2", theta, "erc20_t" + theta.replace(".", ""), FIXED_CORES
    for panel, scenario in (("a1", "erc20_t099"), ("a3", "dmix_h20")):
        for cores in CORES:
            yield panel, str(cores), scenario, cores
    sweeps = (("a4", "h", 20, (0, 2, 5, 10, 20, 40, 60, 80, 100)), ("a5", "w", 50, (10, 25, 50, 75, 100)),
              ("a6", "p", 20, (0, 10, 20, 40, 60, 80, 100)), ("an", "n", 16, (4, 8, 16, 32, 64)))
    for panel, parameter, default, values in sweeps:
        for value in values:
            scenario = "dmix_h20" if value == default else f"dmix_{parameter}{value}"
            yield panel, str(value), scenario, FIXED_CORES


def export_a(options):
    rows = []
    for panel, x, scenario, cores in synth_panels():
        path_of = lambda system, p: os.path.join(options.results, "synth", f"{scenario}_{system}_p{p}.jsonl")
        for system, label in SYSTEMS.items():
            rows.append([panel, label, x, *measured_row(path_of, system, cores)])
    write_csv(options, "figa_synth", "col system x wall_s speedup abort_rate".split(), rows)


# Figure B

def classify(transaction):
    """What a transaction did; units are `to` and every log emitter, so contracts it only called are unseen."""
    category, units = transaction["cat"], len(transaction.get("units", []))
    if category == "failed":
        return "failed"
    if category == "eth":
        return "eth"
    if category == "nft":
        return "nft"
    if category == "swap":
        return "swap" if units <= 4 else "defi"  # one pool: router, pool, two tokens
    if category in ("lending", "userop") or units >= 2:
        return "defi"
    return "token" if category == "token" else "single"


def event_measures(record):
    """Dependency width and conflict-free share (%) of a block, from its event-derived keys."""
    miner = "acct:" + record["miner"]
    chain_end, total, longest, touches, transactions = {}, 0, 1, collections.Counter(), []
    for transaction in record["txs"]:
        gas = max(transaction["gas"], 21000)
        total += gas
        keys = [key for key in transaction["keys"] if key != miner]
        transactions.append(keys)
        finish = max((chain_end[key] for key in keys if key in chain_end), default=0) + gas
        for key in keys:
            chain_end[key] = finish
        longest = max(longest, finish)
        touches.update(set(keys))
    free = sum(1 for keys in transactions if all(touches[key] == 1 for key in keys))
    return total / longest, 100 * free / max(1, len(transactions))


def known_contracts(blocks):
    """Addresses called with calldata or with storage accessed anywhere in the trace."""
    contracts = set()
    for block in blocks:
        for transaction in block["txs"]:
            if transaction["to"] and len(transaction["sel"]) == 10:
                contracts.add(transaction["to"])
            contracts.update(key.split(":")[1] for key in transaction["R"] if key.startswith("s:"))
    return contracts


def prepared_transactions(block, contracts):
    """(gas, reads, writes, after_all) per transaction. Fee payment is left out, a transaction sent by
    the fee recipient follows all earlier ones, and an account load counts as a read only for the
    sender, an account also written, or an account that is not a contract."""
    miner_key = "a:" + block["miner"]
    prepared = []
    for transaction in block["txs"]:
        writes = set(transaction["W"]) - {miner_key}
        reads = set()
        for key in transaction["R"]:
            if key == miner_key:
                continue
            if key.startswith("a:") and not (key[2:] == transaction["from"] or key in writes or key[2:] not in contracts):
                continue
            reads.add(key)
        reads |= {key for key in writes if key.startswith("a:")}
        prepared.append((max(transaction["gas"], 21000), reads, writes, transaction["from"] == block["miner"]))
    return prepared


def exact_measures(transactions):
    """Dependency width and conflict-free share (%) of a block, from its exact read and write sets."""
    chain_end, total, longest = {}, 0, 1
    readers, writers = collections.defaultdict(list), collections.defaultdict(list)
    for index, (gas, reads, writes, after_all) in enumerate(transactions):
        total += gas
        finish = max((chain_end[key] for key in reads if key in chain_end), default=0)
        if after_all:
            finish = max(finish, longest)
        finish += gas
        for key in writes:
            chain_end[key] = finish
            writers[key].append(index)
        longest = max(longest, finish)
        for key in reads:
            readers[key].append(index)
    dependent = set()
    for key, indices in writers.items():
        for reader in readers.get(key, ()):
            lower = [writer for writer in indices if writer < reader]
            if lower:
                dependent.update((reader, max(lower)))
    return total / longest, 100 * (len(transactions) - len(dependent)) / max(1, len(transactions))


def export_b(options):
    kinds = ["eth", "token", "single", "swap", "defi", "nft", "failed"]
    mix_rows, trace_rows = [], []
    for trace in TRACES:
        counts = collections.Counter()
        events = []
        for number in trace_blocks(trace):
            record = read_block("screen", number, options.data)
            counts.update(classify(transaction) for transaction in record["txs"])
            if len(record["txs"]) >= SAMPLE_MINIMUM_TRANSACTIONS:
                events.append(event_measures(record))
        total = sum(counts.values())
        mix_rows.append([trace] + [round(100 * counts[kind] / total, 1) for kind in kinds])
        blocks = [read_block("rwsets", number, options.data) for number in trace_blocks(trace)]
        contracts = known_contracts(blocks)
        exact = [exact_measures(prepared_transactions(block, contracts)) for block in blocks]
        trace_rows.append([trace, round(statistics.median(x[0] for x in exact), 2),
                           round(statistics.median(x[1] for x in exact), 1),
                           round(statistics.median(x[0] for x in events), 2),
                           round(statistics.median(x[1] for x in events), 1)])
    sample_rows = []
    for number in SAMPLE:
        if trace_of(number) is None:
            record = read_block("screen", number, options.data)
            if len(record["txs"]) >= SAMPLE_MINIMUM_TRANSACTIONS:
                width, free = event_measures(record)
                sample_rows.append([number, round(width, 2), round(free, 1)])
    write_csv(options, "figb_mix", ["trace"] + kinds, mix_rows)
    write_csv(options, "figb_traces", "trace exact_width exact_free_pct event_width event_free_pct".split(), trace_rows)
    write_csv(options, "figb_sample", "block event_width event_free_pct".split(), sample_rows)


# Figure C

def scaling_path(options, trace, system, cores):
    return os.path.join(options.results, "scaling", f"{trace}_{system}_p{cores}.jsonl")


def region_transactions(options, trace, transaction_ns):
    """(steps, serial ns, {key: (first access, last write or -1)}) in order, keeping the edges of the
    limit: a write counts only if it changed the key, and account edges of the sender, of the fee
    recipient and of plain payments (no steps) are dropped."""
    transactions, at = [], 0
    for number in trace_blocks(trace):
        with open(os.path.join(options.data, "positions", f"{number}.json")) as source:
            positions = json.load(source)
        block = read_block("rwsets", number, options.data)
        miner = "a:" + block["miner"].lower()
        recorded = {transaction["i"]: transaction for transaction in block["txs"]}
        for offset, position in enumerate(positions):
            transaction = recorded[position["i"]]
            sender = "a:" + transaction["from"].lower()
            changed = {key.lower() for key in transaction["W"]}
            keys = {}
            for key, (first, last) in position["k"].items():
                lowered = key.lower()
                if lowered.startswith("a:") and (lowered in (miner, sender) or position["n"] == 0):
                    continue
                keys[key] = (first, last if last >= 0 and lowered in changed else -1)
            transactions.append((max(position["n"], 1), max(transaction_ns[at + offset], 1), keys))
        at += len(positions)
    return transactions


def makespan(transactions, workers, commit_ns):
    """Time of an ideal region-level run of one window: transactions are list-scheduled in order, a
    reader's access waits for the writer's last write to the key, and the commit is serial and in order."""
    last_write = {}
    starts, durations = [], []
    free = [0.0] * workers
    committed = 0.0
    for steps, duration, keys in transactions:
        start = 0.0
        for key, (first, _) in keys.items():
            if key in last_write:
                writer, share = last_write[key]
                start = max(start, starts[writer] + share * durations[writer] - first / steps * duration)
        start = max(start, heapq.heappop(free))
        heapq.heappush(free, start + duration)
        committed = max(committed, start + duration) + commit_ns
        for key, (_, last) in keys.items():
            if last >= 0:
                last_write[key] = (len(starts), last / steps)
        starts.append(start)
        durations.append(duration)
    return committed


def limit_curve(options, trace):
    serial = read_rows(os.path.join(options.results, "positions", f"{trace}.jsonl"))[0]
    transactions = region_transactions(options, trace, serial["tx_ns"])
    total = sum(duration for _, duration, _ in transactions)
    commit_ns = serial["p1_commit_ns"] / serial["serial_ns"] * total / len(transactions)
    single_core = read_rows(scaling_path(options, trace, "regionfence", 1))[0]["p1_ns"]
    return [min(cores, single_core / makespan(transactions, cores, commit_ns)) for cores in CORES]


def export_c(options):
    curve_rows, limit_rows = [], []
    for trace in TRACES:
        path_of = lambda system, cores: scaling_path(options, trace, system, cores)
        for system, label in SYSTEMS.items():
            curve_rows += [[trace, label, cores, *measured_row(path_of, system, cores)] for cores in CORES]
        limit_rows += [[trace, cores, round(limit, 2)] for cores, limit in zip(CORES, limit_curve(options, trace))]
    write_csv(options, "figc_curves", "trace system cores wall_s speedup abort_rate".split(), curve_rows)
    write_csv(options, "figc_bound", "trace cores limit".split(), limit_rows)


# Figure D

def block_times(options, trace, system, cores):
    """Time spent on each block: RegionFence reports completion times, the baselines per-block walls."""
    times = read_rows(scaling_path(options, trace, system, cores))[0][f"p{cores}_block_ns"]
    if system == "regionfence":
        return [later - earlier for earlier, later in zip([0] + times, times)]
    return times


def export_d(options):
    rows = []
    for trace in TRACES:
        for system, label in SYSTEMS.items():
            single = block_times(options, trace, system, 1)
            parallel = block_times(options, trace, system, FIXED_CORES)
            for start in range(len(single) - SLIDING_BLOCKS + 1):
                end = start + SLIDING_BLOCKS
                rows.append([trace, label, start, round(sum(single[start:end]) / sum(parallel[start:end]), 3)])
    write_csv(options, "figd_rolling", "trace system start_block speedup".split(), rows)


# Figures E, F and G

class Timeline:
    """The events of the mechanism run. Transactions are numbered within a window, so a transaction
    number identifies one only together with its block's trace."""

    def __init__(self, path):
        self.runs = collections.defaultdict(dict)  # (b, j) -> {incarnation: (start, end)}
        self.blocked = collections.defaultdict(list)  # (b, j) -> [(blocked, slot back)]
        self.commits = {}  # (b, j) -> time
        self.releases = []  # (b, j, address, slot, time)
        self.accesses = []  # (b, j, incarnation, address, slot, time, unfinished writers named)
        self.aborts = collections.defaultdict(list)  # (b, j, incarnation, address, slot) -> [time]
        self.waits = collections.defaultdict(list)  # (b, j, incarnation, address, slot) -> [(writer, changed)]
        with open(path) as source:
            for line in source:
                self.parse(line.split())
        self.first_access = {}
        for block, tx, incarnation, address, slot, time, _ in self.accesses:
            self.first_access.setdefault((block, tx, incarnation, address, slot), time)

    def parse(self, f):
        kind = f[0]
        if kind == "S":
            self.runs[int(f[1]), int(f[2])][int(f[3])] = (int(f[4]), int(f[5]))
        elif kind == "B":
            self.blocked[int(f[1]), int(f[2])].append((int(f[4]), int(f[6])))
        elif kind == "C":
            self.commits[int(f[1]), int(f[2])] = int(f[3])
        elif kind == "R":
            self.releases.append((int(f[1]), int(f[2]), f[4].lower(), f[5].lower(), int(f[6])))
        elif kind == "N":
            candidates = [candidate.split(":") for candidate in f[8:]]
            unfinished = [int(c[0]) for c in candidates if c[3] == "false"]
            self.accesses.append((int(f[1]), int(f[2]), int(f[3]), f[4].lower(), f[5].lower(), int(f[6]), unfinished))
        elif kind == "A" and len(f) >= 13 and f[5] == STORAGE_KIND:
            self.aborts[int(f[1]), int(f[2]), int(f[3]), f[10].lower(), f[12].lower()].append(int(f[9]))
        elif kind == "D":
            self.waits[int(f[1]), int(f[2]), int(f[3]), f[5].lower(), f[6].lower()].append((int(f[4]), f[10] == "true"))

    def blocked_time(self, block, tx, low, high):
        return sum(max(0, min(end, high) - max(start, low)) for start, end in self.blocked.get((block, tx), ()))


def region_shares(timeline):
    """Per trace, the critical region of each released key in a committed incarnation, from the first
    access to the last write, both net of blocking, as a share (%) of the incarnation's running time."""
    last_release = {}
    for block, tx, address, slot, time in timeline.releases:
        for incarnation, (start, end) in timeline.runs.get((block, tx), {}).items():
            if start <= time <= end:
                last_release[block, tx, incarnation, address, slot] = time
    shares = collections.defaultdict(list)
    for key, time in last_release.items():
        block, tx, incarnation = key[:3]
        runs = timeline.runs[block, tx]
        first = timeline.first_access.get(key)
        if incarnation != max(runs) or first is None:
            continue
        start, end = runs[incarnation]
        running = end - start - timeline.blocked_time(block, tx, start, end)
        if running > 0:
            shares[trace_of(block)].append(100 * max(0, time - first - timeline.blocked_time(block, tx, first, time)) / running)
    return shares


def region_rows(timeline):
    rows = []
    shares = region_shares(timeline)
    for trace in TRACES:
        values = sorted(shares[trace])
        if values:
            quantile = lambda share: round(values[min(len(values) - 1, int(share * len(values)))], 1)
            rows.append([trace, len(values), *map(quantile, (.05, .25, .5, .75, .95)), round(sum(values) / len(values), 1)])
    return rows


def decision_rows(timeline):
    """One decision per key and incarnation, at its first access. A wait that still ended in an
    abort on the same key is not counted as a right wait."""
    decisions = collections.defaultdict(collections.Counter)
    seen = set()
    for block, tx, incarnation, address, slot, _, unfinished in timeline.accesses:
        key = (block, tx, incarnation, address, slot)
        if key in seen:
            continue
        seen.add(key)
        waits, aborted = timeline.waits.get(key), key in timeline.aborts
        if waits:
            outcome = "waited_then_aborted" if aborted else "wait_right" if any(c for _, c in waits) else "wait_vain"
        elif aborted:
            outcome = "spec_wrong_named" if unfinished else "spec_wrong_unnamed"
        elif unfinished:
            outcome = "spec_right"
        else:
            continue
        decisions[trace_of(block)][outcome] += 1
    names = ["wait_right", "spec_right", "wait_vain", "spec_wrong_named", "spec_wrong_unnamed", "waited_then_aborted"]
    return ["trace"] + names, [[trace] + [decisions[trace][name] for name in names] for trace in TRACES if decisions[trace]]


def key_rows(timeline, data):
    """Per storage key with enough fence events: how often a named unfinished writer really changed
    it, and how often the reader waited."""
    first_tx = {}
    for block, tx in timeline.runs:
        first_tx[block] = min(first_tx.get(block, tx), tx)
    written = {}
    for block in first_tx:
        for transaction in read_block("rwsets", block, data)["txs"]:
            written[trace_of(block), first_tx[block] + transaction["i"]] = frozenset(
                tuple(key.split(":")[1:]) for key in transaction["W"] if key.startswith("s:"))
    counts = collections.defaultdict(lambda: [0, 0, 0])
    seen = set()
    for block, tx, incarnation, address, slot, _, unfinished in timeline.accesses:
        key = (block, tx, incarnation, address, slot)
        if key in seen or not unfinished:
            continue
        seen.add(key)
        trace = trace_of(block)
        entry = counts[address, slot]
        entry[0] += 1
        entry[1] += any((address, slot) in written.get((trace, writer), ()) for writer in unfinished)
        entry[2] += key in timeline.waits
    return [[events, changed, waited, round(changed / events, 4), round(100 * waited / events, 2)]
            for events, changed, waited in counts.values() if events >= MINIMUM_FENCE_EVENTS]


def gantt_rows(timeline):
    """The first transactions of the conflict chain with the most waits on a real writer."""
    chains = collections.Counter()
    for (block, _, _, address, slot), waits in timeline.waits.items():
        if any(changed for _, changed in waits):
            chains[block, address, slot] += 1
    (block, address, slot), _ = chains.most_common(1)[0]
    members = set()
    for key, waits in timeline.waits.items():
        if (key[0], key[3], key[4]) == (block, address, slot):
            members.add(key[1])
            members.update(writer for writer, _ in waits)
    chain = sorted(tx for tx in members if (block, tx) in timeline.runs)[:GANTT_TRANSACTIONS]
    origin = min(start for tx in chain for start, _ in timeline.runs[block, tx].values())
    microseconds = lambda time: round((time - origin) / 1e3, 1)
    released = {tx: time for b, tx, a, s, time in timeline.releases if (b, a, s) == (block, address, slot)}
    rows = []
    for row, tx in enumerate(chain, 1):
        for incarnation, (start, end) in sorted(timeline.runs[block, tx].items()):
            rows.append([row, tx, incarnation, "run", microseconds(start), microseconds(end)])
            for blocked, resumed in timeline.blocked.get((block, tx), ()):
                if start <= blocked <= end:
                    rows.append([row, tx, incarnation, "block", microseconds(blocked), microseconds(min(resumed, end))])
            first = timeline.first_access.get((block, tx, incarnation, address, slot))
            if first:
                rows.append([row, tx, incarnation, "first", microseconds(first), ""])
            if tx in released and start <= released[tx] <= end:
                rows.append([row, tx, incarnation, "release", microseconds(released[tx]), ""])
        if (block, tx) in timeline.commits:
            rows.append([row, tx, "", "commit", microseconds(timeline.commits[block, tx]), ""])
    return rows


def hit_rows(timeline):
    waits, hits = collections.Counter(), collections.Counter()
    for key, outcomes in timeline.waits.items():
        waits[key[0]] += len(outcomes)
        hits[key[0]] += sum(changed for _, changed in outcomes)
    numbers = [number for trace in TRACES for number in trace_blocks(trace)]
    return [[index, number, trace_of(number), waits[number], hits[number]] for index, number in enumerate(numbers)]


def width_rows(rows):
    return [[trace_of(row["block"]), index, width, round(row[f"p{FIXED_CORES}_aborts"] / row[f"p{FIXED_CORES}_exec"], 4)]
            for row in rows for index, width in enumerate(row[f"p{FIXED_CORES}_window_width"])]


def export_mechanism(options):
    directory = os.path.join(options.results, "mechanism")
    timeline = Timeline(os.path.join(directory, "timeline.txt"))
    write_csv(options, "fige_gantt", "row tx inc seg t0_us t1_us".split(), gantt_rows(timeline))
    write_csv(options, "fige_region", "trace n q05 q25 q50 q75 q95 mean".split(), region_rows(timeline))
    write_csv(options, "figf_decisions", *decision_rows(timeline))
    write_csv(options, "figf_keys", "fence_events written waits p_written wait_pct".split(), key_rows(timeline, options.data))
    write_csv(options, "figg_hits", "idx block trace waits hits".split(), hit_rows(timeline))
    write_csv(options, "figg_width", "trace block_idx width abort_rate".split(),
              width_rows(read_rows(os.path.join(directory, "mechanism.jsonl"))))


def export_control(options):
    rows = []
    for trace in TRACES:
        for variant in CONTROLS:
            row = read_rows(os.path.join(options.results, "control", f"{trace}_{variant}.jsonl"))[0]
            executions, aborts = row[f"p{FIXED_CORES}_exec"], row[f"p{FIXED_CORES}_aborts"]
            rows.append([trace, variant, round(row[f"p{FIXED_CORES}_ns"] / 1e9, 4), round(aborts / executions, 4), executions, aborts])
    write_csv(options, "figg_effect", "trace variant wall_s abort_rate exec aborts".split(), rows)


EXPORTS = {"a": export_a, "b": export_b, "c": export_c, "d": export_d,
           "mechanism": export_mechanism, "control": export_control}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("exports", nargs="*", help="any of a, b, c, d, mechanism, control; default: all")
    parser.add_argument("--results", default=RESULTS)
    parser.add_argument("--data", default=DATA)
    parser.add_argument("--out", default=os.path.join(ROOT, "figures", "data"))
    options = parser.parse_args()
    os.makedirs(options.out, exist_ok=True)
    for name in options.exports or EXPORTS:
        EXPORTS[name](options)


if __name__ == "__main__":
    main()
