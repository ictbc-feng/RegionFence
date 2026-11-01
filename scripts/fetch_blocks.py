#!/usr/bin/env python3
"""Fetch the mainnet data of the experiments over the Ethereum JSON-RPC API (Alchemy).

Every block of the nine traces gets
  data/rwsets/<block>.json.gz  per-transaction read and write sets, keys a:<addr> (balance and
                               nonce), c:<addr> (code) and s:<addr>:<slot> (storage)
  data/state/<block>.json.gz   the raw block, the pre-state at each key's first touch (code in
                               data/state/code/<sha256>.hex), the post-state, the receipts
  data/screen/<block>.json.gz  the transaction summaries of the screening
The blocks of the chain sample get only the screening summary, and data/state/hashes.json maps
block numbers to hashes for BLOCKHASH. Existing files are skipped, so a rerun fills the gaps.

Usage: ALCHEMY_API_KEY=... python3 scripts/fetch_blocks.py [--workers 12]
"""
import argparse
import gzip
import hashlib
import json
import os
import sys
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

from traces import DATA, TRACES, all_trace_blocks, read_block

RWSETS = os.path.join(DATA, "rwsets")
STATE = os.path.join(DATA, "state")
SCREEN = os.path.join(DATA, "screen")
CODE = os.path.join(STATE, "code")
HASHES = os.path.join(STATE, "hashes.json")
SAMPLE = range(12000000, 26060001, 10000)
RETRIES = 5

TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
APPROVAL = "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"
SYNC_V2 = "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"
SWAP_V2 = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
SWAP_V3 = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
SWAP_V4 = "0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f"
SWAP_BALANCER = "0x2170c741c41531aec20e7c107c24eecfdd15e69c9bb0a8dd37b1840b9e0b207b"
SWAP_CURVE = "0x8b3e96f2b889fa771c53c981b40daf005f63f637f1869f707052d15a3dd97140"
SWAPS = {SWAP_V2, SWAP_V3, SWAP_V4, SWAP_BALANCER, SWAP_CURVE}
WETH_DEPOSIT = "0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c"
WETH_WITHDRAWAL = "0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65"
USER_OPERATION = "0x49628fd1471006c1482da88028e9ce4dbb080b815c9b0344d39e5a8e6ec1419f"
ERC1155_EVENTS = {
    "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62",
    "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb",
    "0x9d9af8e38d66c62e2c12f0225249fd9d721c54b83f48d9352c97c6cacdcb6f31",  # Seaport
}
AAVE_EVENTS = {
    "0x2b627736bca15cd5381dcf80b0bf11fd197d01a037c52b927a881a10fb73ba61",
    "0xb3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0",
    "0xa534c8dbe71f871f9f3530e97a74601fea17b426cae02e1c5aee42c96c784051",
    "0xe413a321e8681d831f4dbccbca790d2952b56f977908e45be37335533e005286",
}
ZERO_ADDRESS = "0x" + "0" * 40
CATEGORIES = ["swap", "lending", "nft", "userop", "weth", "token", "other-events", "eth", "call-nolog", "failed"]


def rpc(url, method, params):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    for attempt in range(RETRIES):
        try:
            request = urllib.request.Request(url, data=body, headers={"content-type": "application/json"})
            with urllib.request.urlopen(request, timeout=120) as response:
                reply = json.loads(response.read())
            if "error" in reply:
                raise RuntimeError(f"{method}: {reply['error']}")
            return reply["result"]
        except Exception:
            if attempt == RETRIES - 1:
                raise
            time.sleep(2 ** attempt + 1)


def write_gzip(path, record):
    temporary = path + ".tmp"
    with gzip.open(temporary, "wt") as sink:
        json.dump(record, sink, separators=(",", ":"))
    os.replace(temporary, path)


def calldata_addresses(calldata, limit=16):
    """Addresses embedded as ABI words in the calldata: a cheap pre-execution hint."""
    body = calldata[10:] if calldata.startswith("0x") else calldata[8:]
    found = []
    for offset in range(0, len(body) - 63, 64):
        word = body[offset:offset + 64]
        if word[:24] == "0" * 24 and word[24:] != "0" * 40 and word[24:32] != "00000000":
            address = "0x" + word[24:]
            if address not in found:
                found.append(address)
                if len(found) >= limit:
                    break
    return found


def storage_keys(address, entry):
    return [f"s:{address}:{slot}" for slot in entry.get("storage") or {}]


def changed_keys(pre, post):
    """The direction of every numeric change: '+' increase, '-' decrease."""
    deltas = {}
    for address in set(post) | set(pre):
        before, after = pre.get(address, {}), post.get(address, {})
        if "balance" in after or "balance" in before:
            old = int(before.get("balance", "0x0"), 16)
            new = int(after.get("balance", before.get("balance", "0x0")), 16)
            if new != old:
                deltas[f"a:{address}"] = "+" if new > old else "-"
        old_slots, new_slots = before.get("storage") or {}, after.get("storage") or {}
        for slot in set(old_slots) | set(new_slots):
            old = int(old_slots.get(slot, "0x0"), 16)
            new = int(new_slots.get(slot, "0x0"), 16)
            if new != old:
                deltas[f"s:{address}:{slot}"] = "+" if new > old else "-"
    return deltas


def rwset_record(number, block, receipts, pre_traces, diff_traces):
    transactions = []
    for index, (tx, receipt, pre_trace, diff_trace) in enumerate(
            zip(block["transactions"], receipts, pre_traces, diff_traces)):
        reads = []
        for address, entry in (pre_trace.get("result") or {}).items():
            reads.append(f"a:{address}")
            reads.extend(storage_keys(address, entry))
        result = diff_trace.get("result") or {}
        pre, post = result.get("pre") or {}, result.get("post") or {}
        writes = []
        for address, entry in post.items():
            if "balance" in entry or "nonce" in entry:
                writes.append(f"a:{address}")
            if "code" in entry or "codeHash" in entry:
                writes.append(f"c:{address}")
            writes.extend(storage_keys(address, entry))
        # An account in pre but not in post was deleted.
        writes.extend(f"a:{address}" for address in pre if address not in post)
        calldata = tx.get("input") or "0x"
        transactions.append({
            "i": index,
            "h": tx["hash"],
            "from": tx["from"],
            "to": tx.get("to"),
            "sel": calldata[:10] if len(calldata) >= 10 else calldata,
            "cda": calldata_addresses(calldata),
            "value": tx.get("value", "0x0") != "0x0",
            "type": int(tx.get("type", "0x0"), 16),
            "gas": int(receipt["gasUsed"], 16),
            "ok": int(receipt.get("status", "0x1"), 16),
            "create": receipt.get("contractAddress"),
            "R": sorted(set(reads)),
            "W": sorted(set(writes)),
            "D": changed_keys(pre, post),
        })
    return {
        "number": number,
        "hash": block["hash"],
        "timestamp": int(block["timestamp"], 16),
        "miner": block["miner"],
        "gasUsed": int(block["gasUsed"], 16),
        "gasLimit": int(block["gasLimit"], 16),
        "baseFee": int(block.get("baseFeePerGas", "0x0"), 16),
        "txs": transactions,
    }


def store_code(code):
    if not code or code == "0x":
        return None
    digest = hashlib.sha256(code.encode()).hexdigest()
    path = os.path.join(CODE, digest + ".hex")
    if not os.path.exists(path):
        temporary = f"{path}.{threading.get_ident()}.tmp"
        with open(temporary, "w") as sink:
            sink.write(code)
        os.replace(temporary, path)
    return digest


def state_record(block, receipts, pre_traces, diff_traces):
    # Pre-state: the first value seen of every account field and slot.
    pre = {}
    for trace in pre_traces:
        for address, account in (trace.get("result") or {}).items():
            entry = pre.get(address)
            if entry is None:
                entry = pre[address] = {"balance": account.get("balance", "0x0"), "nonce": account.get("nonce", 0),
                                        "code": store_code(account.get("code")), "storage": {}}
            for slot, value in (account.get("storage") or {}).items():
                entry["storage"].setdefault(slot, value)
    # Post-state: the last written value of every written key.
    post = {}
    for trace in diff_traces:
        result = trace.get("result") or {}
        before_all, after_all = result.get("pre") or {}, result.get("post") or {}
        for address in set(before_all) | set(after_all):
            before, after = before_all.get(address, {}), after_all.get(address)
            entry = post.setdefault(address, {"storage": {}})
            if after is None:
                post[address] = {"deleted": True, "storage": {}}
                continue
            entry.pop("deleted", None)
            for field in ("balance", "nonce"):
                if field in after:
                    entry[field] = after[field]
            if "code" in after or "codeHash" in after:
                entry["codeChanged"] = True
            old_slots, new_slots = before.get("storage") or {}, after.get("storage") or {}
            for slot in old_slots:
                if slot not in new_slots:
                    entry["storage"][slot] = "0x" + "0" * 64
            entry["storage"].update(new_slots)
    return {
        "block": block,
        "pre": pre,
        "post": post,
        "receipts": [{"gasUsed": receipt["gasUsed"], "status": receipt.get("status", "0x1"),
                      "cumulativeGasUsed": receipt["cumulativeGasUsed"]} for receipt in receipts],
    }


def topic_address(topic):
    return "0x" + topic[-40:]


def balance_keys(token, topics):
    keys = set()
    for topic in topics:
        holder = topic_address(topic)
        keys.add(f"supply:{token}" if holder == ZERO_ADDRESS else f"bal:{token}:{holder}")
    return keys


def summarize_transaction(tx, receipt):
    """What a transaction touched at contract level (units) and, from its events, at key level."""
    recipient = (tx.get("to") or "").lower()
    sender = tx["from"].lower()
    calldata = tx.get("input") or "0x"
    if not recipient:
        kind = "create"
    elif len(calldata) < 10:
        kind = "eth"
    else:
        kind = recipient + ":" + calldata[:10]
    units, keys, categories = set(), {"acct:" + sender}, set()
    if recipient and len(calldata) >= 10:
        units.add(recipient)
    if recipient and int(tx.get("value", "0x0"), 16) > 0:
        keys.add("acct:" + recipient)
    for log in receipt.get("logs", []):
        emitter = log["address"].lower()
        topics = log.get("topics") or []
        event = topics[0] if topics else ""
        unit = emitter
        # Singleton pools name the pool in a topic.
        if event in (SWAP_V4, SWAP_BALANCER) and len(topics) > 1:
            unit = emitter + ":" + topics[1]
        units.add(unit)
        if event in SWAPS:
            categories.add("swap")
            keys.add("pool:" + unit)
        elif event == SYNC_V2:
            keys.add("pool:" + emitter)
        elif event in AAVE_EVENTS:
            categories.add("lending")
            keys.add("reserve:" + emitter + ":" + (topics[1] if len(topics) > 1 else ""))
        elif event == TRANSFER and len(topics) == 4:  # ERC-721
            categories.add("nft")
            keys.add("nft:" + emitter + ":" + topics[3])
            keys |= balance_keys(emitter, topics[1:3])
        elif event in ERC1155_EVENTS:
            categories.add("nft")
            keys.add("nftx:" + emitter)
        elif event == TRANSFER and len(topics) == 3:  # ERC-20
            categories.add("token")
            keys |= balance_keys(emitter, topics[1:3])
        elif event in (WETH_DEPOSIT, WETH_WITHDRAWAL) and len(topics) > 1:
            categories.add("weth")
            keys.add("ethbal:" + emitter)
            keys.add("bal:" + emitter + ":" + topic_address(topics[1]))
        elif event == USER_OPERATION:
            categories.add("userop")
            keys.add("ethbal:" + emitter)
            if len(topics) > 2:
                keys.add("nonce4337:" + topic_address(topics[2]))
        elif event == APPROVAL and len(topics) == 3:
            keys.add("allow:" + emitter + ":" + topic_address(topics[1]) + ":" + topic_address(topics[2]))
        else:
            categories.add("other-events")
    if int(receipt.get("status", "0x1"), 16) == 0:
        category = "failed"
    else:
        fallback = "eth" if kind == "eth" else "call-nolog"
        category = next((name for name in CATEGORIES if name in categories), fallback)
    return {"kind": kind, "cat": category, "gas": int(receipt["gasUsed"], 16),
            "units": sorted(units), "keys": sorted(keys), "from": sender}


def screen_record(number, block, receipts):
    by_hash = {receipt["transactionHash"]: receipt for receipt in receipts}
    return {"number": number, "timestamp": int(block["timestamp"], 16), "miner": block["miner"].lower(),
            "gasUsed": int(block["gasUsed"], 16),
            "txs": [summarize_transaction(tx, by_hash[tx["hash"]]) for tx in block["transactions"]]}


def fetch_block(url, number, traced):
    """Write the files of a block that are missing; only traced blocks have rw-sets and state."""
    rwset_path = os.path.join(RWSETS, f"{number}.json.gz")
    state_path = os.path.join(STATE, f"{number}.json.gz")
    screen_path = os.path.join(SCREEN, f"{number}.json.gz")
    need_traces = traced and not (os.path.exists(rwset_path) and os.path.exists(state_path))
    if not need_traces and os.path.exists(screen_path):
        return
    tag = hex(number)
    block = rpc(url, "eth_getBlockByNumber", [tag, True])
    receipts = rpc(url, "eth_getBlockReceipts", [tag])
    if need_traces:
        # The code in the first trace is ignored by the rw-sets and kept by the state.
        pre_traces = rpc(url, "debug_traceBlockByNumber",
                         [tag, {"tracer": "prestateTracer", "tracerConfig": {"diffMode": False}}])
        diff_traces = rpc(url, "debug_traceBlockByNumber",
                          [tag, {"tracer": "prestateTracer", "tracerConfig": {"diffMode": True, "disableCode": True}}])
        assert len(block["transactions"]) == len(receipts) == len(pre_traces) == len(diff_traces), number
        if not os.path.exists(rwset_path):
            write_gzip(rwset_path, rwset_record(number, block, receipts, pre_traces, diff_traces))
        if not os.path.exists(state_path):
            write_gzip(state_path, state_record(block, receipts, pre_traces, diff_traces))
    if not os.path.exists(screen_path):
        write_gzip(screen_path, screen_record(number, block, receipts))


def fetch_hashes(url, workers):
    """The 256 hashes before each trace, for BLOCKHASH, and those of the traces themselves."""
    hashes = {}
    if os.path.exists(HASHES):
        with open(HASHES) as source:
            hashes = json.load(source)
    wanted = [number for first in TRACES.values() for number in range(first - 256, first)]
    missing = [number for number in wanted if str(number) not in hashes]
    with ThreadPoolExecutor(workers) as pool:
        headers = pool.map(lambda number: rpc(url, "eth_getBlockByNumber", [hex(number), False]), missing)
        for number, header in zip(missing, headers):
            hashes[str(number)] = header["hash"]
    for number in all_trace_blocks():
        hashes[str(number)] = read_block("rwsets", number)["hash"]
    with open(HASHES, "w") as sink:
        json.dump(hashes, sink)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--workers", type=int, default=12)
    workers = parser.parse_args().workers
    key = os.environ.get("ALCHEMY_API_KEY")
    if not key:
        sys.exit("ALCHEMY_API_KEY is not set")
    url = f"https://eth-mainnet.g.alchemy.com/v2/{key}"
    for directory in (RWSETS, CODE, SCREEN):
        os.makedirs(directory, exist_ok=True)
    traced = set(all_trace_blocks())
    failed = 0
    with ThreadPoolExecutor(workers) as pool:
        futures = {pool.submit(fetch_block, url, number, number in traced): number
                   for number in sorted(traced | set(SAMPLE))}
        for future in as_completed(futures):
            if future.exception():
                failed += 1
                print(f"block {futures[future]}: {future.exception()}", file=sys.stderr)
    if failed:
        sys.exit(f"{failed} blocks failed; rerun to fill the gaps")
    fetch_hashes(url, workers)


if __name__ == "__main__":
    main()
