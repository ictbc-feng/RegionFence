#!/usr/bin/env python3
"""Run the experiments of the paper with ./rfreplay, one subcommand per experiment.

  synth      Figure A: the synthetic sweeps, 1 and 64 cores, all core counts at the defaults
  scaling    Figures C and D: 1 to 128 cores on every trace, for RegionFence, OCC and PCC
  control    Figure G(a): 64 cores, the default against each learning mechanism turned off
  mechanism  Figures E, F and G(b, c): the nine traces back to back on 64 cores, with a time-line
  positions  Figure C's limit: serial transaction times and access positions, on one core

Output goes to results/<experiment>/<item>.jsonl and items already there are skipped, so a rerun
resumes. Every item has strictly P cores and P workers, on the memory of NUMA node 0. Fix the CPU
frequency before running: the wall clocks are compared across core counts.
"""
import argparse
import os
import re
import subprocess
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))
from traces import RESULTS, ROOT, TRACE_LENGTH, TRACES, all_trace_blocks

CORES = (1, 2, 4, 8, 16, 32, 64, 128)
FIXED_CORES = 64
SYSTEMS = {"regionfence": [], "occ": ["-cc", "occ"], "pcc": ["-cc", "pcc"]}
CONTROLS = {"default": [], "no_cross_block": ["-no-cross-block"], "no_learned_waits": ["-no-learned-waits"]}
RUNTIME = ["-gogc", "400", "-memlimit", "65536", "-window", "100"]
TIMED = ["-repeat", "3", "-noserial"]
SYNTH_FIRST_BLOCK = 30000000
SYNTH_DEFAULTS = ("erc20_t099", "dmix_h20")
SYNTH_POINTS = (
    [f"erc20_t{theta}" for theta in ("0", "04", "06", "08", "09", "099", "12")]
    + [f"dmix_h{hot}" for hot in (0, 2, 5, 10, 20, 40, 60, 80, 100)]
    + [f"dmix_w{write}" for write in (10, 25, 75, 100)]
    + [f"dmix_p{position}" for position in (0, 10, 40, 60, 80, 100)]
    + [f"dmix_n{slots}" for slots in (4, 8, 32, 64)]
)


def trace_range(trace):
    first = TRACES[trace]
    return ["-dir", "data/state", "-from", str(first), "-to", str(first + TRACE_LENGTH - 1)]


def require_performance_governor():
    """Other governors nearly double the wall time at 64 cores while single-core runs look fine."""
    path = "/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"
    if os.path.exists(path) and open(path).read().strip() != "performance":
        sys.exit("set the performance governor first: cpupower frequency-set -g performance")


def run_item(experiment, name, cores, arguments, options):
    """Run one item; its output appears under its final name only when it passed."""
    directory = os.path.join(RESULTS, experiment)
    output = os.path.join(directory, f"{name}.jsonl")
    if os.path.exists(output):
        return
    command = ["numactl", "--membind=0", "taskset", "-c", f"0-{cores - 1}", options.binary,
               *arguments, "-threads", str(cores), "-out", output + ".tmp"]
    if options.dry_run:
        print(f"GOMAXPROCS={cores} GODEBUG=gcshrinkstackoff=1", *command)
        return
    os.makedirs(directory, exist_ok=True)
    print(f"{experiment}/{name}", flush=True)
    environment = dict(os.environ, GOMAXPROCS=str(cores), GODEBUG="gcshrinkstackoff=1")
    result = subprocess.run(command, cwd=ROOT, env=environment, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True)
    with open(os.path.join(directory, f"{name}.log"), "w") as log:
        log.write(result.stdout)
    differing = re.findall(r"(\d+) (?:block-configurations )?differ\b", result.stdout)
    if result.returncode != 0 or any(int(count) for count in differing):
        sys.exit(f"{experiment}/{name} failed: exit {result.returncode}, differ {differing}")
    os.replace(output + ".tmp", output)


def synth(options):
    for scenario in SYNTH_POINTS:
        arguments = ["-dir", f"data/synth/{scenario}", "-from", str(SYNTH_FIRST_BLOCK),
                     "-to", str(SYNTH_FIRST_BLOCK + TRACE_LENGTH - 1), *RUNTIME, *TIMED]
        for cores in CORES if scenario in SYNTH_DEFAULTS else (1, FIXED_CORES):
            for system, flags in SYSTEMS.items():
                run_item("synth", f"{scenario}_{system}_p{cores}", cores, arguments + flags, options)


def scaling(options):
    for trace in TRACES:
        for cores in CORES:
            for system, flags in SYSTEMS.items():
                run_item("scaling", f"{trace}_{system}_p{cores}", cores,
                         trace_range(trace) + RUNTIME + TIMED + flags, options)


def control(options):
    for trace in TRACES:
        for variant, flags in CONTROLS.items():
            run_item("control", f"{trace}_{variant}", FIXED_CORES,
                     trace_range(trace) + RUNTIME + TIMED + flags, options)


def mechanism(options):
    directory = os.path.join(RESULTS, "mechanism")
    blocks = all_trace_blocks()
    block_list = os.path.join(directory, "blocks.txt")
    if not options.dry_run:
        os.makedirs(directory, exist_ok=True)
        with open(block_list, "w") as sink:
            sink.writelines(f"{number}\n" for number in blocks)
    run_item("mechanism", "mechanism", FIXED_CORES,
             ["-dir", "data/state", "-from", str(blocks[0]), "-to", str(blocks[-1]), "-blocks", block_list,
              *RUNTIME, "-repeat", "1", "-noserial", "-timeline", os.path.join(directory, "timeline.txt")],
             options)


def positions(options):
    for trace in TRACES:
        run_item("positions", trace, 1,
                 trace_range(trace) + RUNTIME + ["-repeat", "3", "-positions", "data/positions"], options)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("experiment", choices=["synth", "scaling", "control", "mechanism", "positions"])
    parser.add_argument("--binary", default="./rfreplay")
    parser.add_argument("--dry-run", action="store_true", help="print the commands of the items still to do")
    options = parser.parse_args()
    if not options.dry_run:
        require_performance_governor()
    globals()[options.experiment](options)


if __name__ == "__main__":
    main()
