#!/bin/bash
# Writes the synthetic workloads of Figure A into data/synth/<name>: 100 blocks of
# 200 transactions each, a fresh sender per transaction and the same seed, so that
# the points of a sweep differ only in their parameter.
#   erc20_t<theta>  transfers to 1000 recipients, Zipf theta (t099 is 0.99)
#   dmix_h<pct>     DeFi-Mix: pct of the transactions on the hot pool
#   dmix_w<pct>     pct of the hot-pool transactions writing (swap, not priceRead)
#   dmix_p<pct>     hot access pct into the transaction
#   dmix_n<count>   storage slots per transaction, with padding in proportion
# DeFi-Mix defaults are h=20, w=50, p=20, n=16, so dmix_h20 is also the default
# point of the other sweeps. Padding brings a transaction to about the cost of a
# mainnet transaction.
set -eu
cd "$(dirname "$0")/.."
[ -x ./rfsynth ] || go build -o rfsynth ./cmd/rfsynth

generate() { # name, then rfsynth arguments
  local name=$1
  shift
  rm -rf "data/synth/$name"
  ./rfsynth -out "data/synth/$name" "$@"
}

erc20_padding=80
for theta in 0 0.4 0.6 0.8 0.9 0.99 1.2; do
  generate "erc20_t${theta//./}" -fn transfer -recipients 1000 -zipf "$theta" -pad "$erc20_padding" -pos 20
done

defi_mix_padding=30 # at 16 slots
defi_mix() { # name hot_pct write_pct slots position_pct
  generate "dmix_$1" -fn defimix -hotfrac "$(awk "BEGIN{print $2/100}")" -writefrac "$(awk "BEGIN{print $3/100}")" \
    -n "$4" -pos "$5" -pad "$((defi_mix_padding * $4 / 16))"
}
for hot in 0 2 5 10 20 40 60 80 100; do defi_mix "h$hot" "$hot" 50 16 20; done
for write in 10 25 75 100; do defi_mix "w$write" 20 "$write" 16 20; done
for position in 0 10 40 60 80 100; do defi_mix "p$position" 20 50 16 "$position"; done
for slots in 4 8 32 64; do defi_mix "n$slots" 20 50 "$slots" 20; done
