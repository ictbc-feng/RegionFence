// SPDX-License-Identifier: LGPL-3.0-or-later
pragma solidity ^0.8.20;

// The contract of the synthetic workloads (rfsynth): each function isolates
// one access pattern of the concurrency control.
//
// priceRead, swap and xfer are the transactions of DeFi-Mix, with one selector
// each so that a key signature can tell the pool's writers from its readers.
// Each touches n distinct storage slots and hashes for pad rounds; pos (0-100)
// places its hot access: pos% of the rounds and of the private slots come
// before it, the rest after it.
contract Synth {
    mapping(address => uint256) public own; // slot 0: per sender, never shared
    uint256[64] public hot;                 // slots 1-64: shared counters
    mapping(address => uint256) public bal; // slot 65: token-style balances
    mapping(address => uint256) public reserves; // slot 66: a pool's two reserves in one word, as in Uniswap V2
    mapping(address => mapping(uint256 => uint256)) private priv; // slot 67: per sender, never shared

    // work hashes for `loops` rounds on the sender's own slot and, if h > 0,
    // increments hot[h-1] before the rounds (first != 0) or after them.
    function work(uint256 loops, uint256 h, uint256 first) external {
        if (h != 0 && first != 0) {
            hot[h - 1] += 1;
        }
        own[msg.sender] = _pad(own[msg.sender], loops);
        if (h != 0 && first == 0) {
            hot[h - 1] += 1;
        }
    }

    // transfer moves amt from the sender's balance to `to`'s, pos% into pad
    // rounds of hashing.
    function transfer(address to, uint256 amt, uint256 pad, uint256 pos) external {
        uint256 before = pad * pos / 100;
        uint256 x = _pad(amt, before);
        bal[msg.sender] -= amt;
        bal[to] += amt;
        _pad(x, pad - before);
    }

    // priceRead only reads the pool, and writes what it derives to the
    // sender's own slots (own, and those of _steps).
    function priceRead(address pool, uint256 n, uint256 pad, uint256 pos) external {
        (uint256 k, uint256 cut, uint256 before) = _split(n - 2, pad, pos);
        uint256 x = _steps(own[msg.sender], before, 0, cut);
        uint256 r = reserves[pool];
        x ^= (r >> 128) * 1e18 / (r & type(uint128).max);
        own[msg.sender] = _steps(x, pad - before, cut, k);
    }

    // swap is two hops: a read-modify-write of the pool's reserves, then, with
    // the amount out, a quiet pool of the sender's own (the rest of _steps).
    // The steps before the swap stand for a first hop elsewhere, as in the
    // paper's T3; at pos 0 there is none, as in T2.
    function swap(address pool, uint256 amt, uint256 n, uint256 pad, uint256 pos) external {
        (uint256 k, uint256 cut, uint256 before) = _split(n - 2, pad, pos);
        uint256 x = _steps(own[msg.sender], before, 0, cut);
        uint256 r = reserves[pool];
        uint256 r0 = r >> 128;
        uint256 r1 = r & type(uint128).max;
        uint256 out = r1 * amt / (r0 + amt);
        reserves[pool] = ((r0 + amt) << 128) | (r1 - out);
        own[msg.sender] = _steps(x ^ out, pad - before, cut, k);
    }

    // xfer is a token transfer unrelated to the pool.
    function xfer(address to, uint256 amt, uint256 n, uint256 pad, uint256 pos) external {
        (uint256 k, uint256 cut, uint256 before) = _split(n - 3, pad, pos);
        uint256 x = _steps(own[msg.sender], before, 0, cut);
        bal[msg.sender] -= amt;
        bal[to] += amt;
        own[msg.sender] = _steps(x, pad - before, cut, k);
    }

    function _split(uint256 k, uint256 pad, uint256 pos) private pure returns (uint256, uint256, uint256) {
        return (k, k * pos / 100, pad * pos / 100);
    }

    // _steps spreads rounds evenly over the sender's private slots [from, to):
    // it reads each into x and writes back every fourth, about mainnet's mix
    // of reads and writes.
    function _steps(uint256 x, uint256 rounds, uint256 from, uint256 to) private returns (uint256) {
        uint256 k = to - from;
        if (k == 0) {
            return _pad(x, rounds);
        }
        mapping(uint256 => uint256) storage m = priv[msg.sender];
        for (uint256 i = 0; i < k; i++) {
            x = _pad(x, rounds * (i + 1) / k - rounds * i / k);
            uint256 s = from + i;
            x ^= m[s];
            if (s % 4 == 0) {
                m[s] = x;
            }
        }
        return x;
    }

    function _pad(uint256 x, uint256 rounds) private pure returns (uint256) {
        for (uint256 i = 0; i < rounds; i++) {
            x = uint256(keccak256(abi.encode(x, i)));
        }
        return x;
    }
}
