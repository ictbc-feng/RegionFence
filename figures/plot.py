#!/usr/bin/env python3
"""Render the figures of the paper from figures/data/*.csv into figures/figure_<letter>.pdf.

Usage: python3 figures/plot.py [a b c d e f g]
"""
import csv
import sys
from pathlib import Path

import matplotlib as mpl

mpl.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
from matplotlib.ticker import ScalarFormatter

HERE = Path(__file__).resolve().parent
COLUMN_WIDTH, TEXT_WIDTH = 3.34, 7.0
TRACES = list("ABCDEFGHI")
SYSTEMS = ["RegionFence", "OCC", "PCC"]

BLUE, SKY, PALE_BLUE = "#1771bf", "#5fb0e6", "#b9dcf4"
RED, PALE_RED, YELLOW = "#bb2205", "#f3c9bf", "#ffd66b"
INK, GRAY, LIGHT_GRAY, SLATE = "#202020", "#5a5a5a", "#d0d0d0", "#394867"
SERIES = {  # color draws lines, fill fills bars and markers
    "RegionFence": dict(color=BLUE, fill=BLUE, width=1.5, marker="^", zorder=4),
    "OCC": dict(color="#3fb89c", fill="#80d8c3", width=1.2, marker="s", zorder=3),
    "PCC": dict(color="#e8b21c", fill=YELLOW, width=1.2, marker="o", zorder=3),
}

mpl.rcParams.update({
    "font.family": "sans-serif", "font.sans-serif": ["Helvetica", "Arial", "DejaVu Sans"],
    "font.size": 7.5, "axes.labelsize": 7.5, "axes.titlesize": 7.5,
    "xtick.labelsize": 7, "ytick.labelsize": 7, "legend.fontsize": 7,
    "axes.linewidth": 0.5, "axes.edgecolor": INK, "axes.labelcolor": INK,
    "xtick.color": INK, "ytick.color": INK,
    "xtick.major.width": 0.5, "ytick.major.width": 0.5, "xtick.major.size": 2.5, "ytick.major.size": 2.5,
    "axes.grid": True, "axes.axisbelow": True,
    "grid.color": "#c8c8c8", "grid.linestyle": "-", "grid.linewidth": 0.4,
    "lines.linewidth": 1.0, "lines.markersize": 3.5, "lines.markeredgewidth": 0.4, "lines.markeredgecolor": "#000000",
    "patch.linewidth": 0.4, "patch.edgecolor": "#000000", "patch.force_edgecolor": True,
    "legend.frameon": True, "legend.framealpha": 0.8, "legend.edgecolor": "#cccccc",
    "legend.fancybox": True, "legend.borderpad": 0.3, "legend.handlelength": 1.8,
    "legend.handletextpad": 0.4, "legend.columnspacing": 1.0,
    "pdf.fonttype": 42, "ps.fonttype": 42, "savefig.bbox": None, "savefig.pad_inches": 0.0,
})


def read(name):
    with open(HERE / "data" / f"{name}.csv") as source:
        return list(csv.DictReader(source))


def system_line(axes, x, y, system, label=None, marker_size=3.2):
    series = SERIES[system]
    axes.plot(x, y, color=series["color"], lw=series["width"], marker=series["marker"], ms=marker_size,
              mfc=series["fill"], mec="black", mew=0.4, zorder=series["zorder"], label=label)


def margins(figure, left, top, bottom, wspace, right=0.985):
    figure.subplots_adjust(left=left, right=right, top=top, bottom=bottom, wspace=wspace)


def cores_axis(axes):
    axes.set_xscale("log", base=2)
    axes.set_xticks([1, 2, 4, 8, 16, 32, 64, 128])
    axes.xaxis.set_major_formatter(ScalarFormatter())
    axes.set_xlim(1 / 1.15, 128 * 1.15)
    axes.set_xlabel("Cores")


def speedup_scale(set_scale, set_ticks, set_labels, axes, labelled=True):
    set_scale("log", base=2)
    axes.minorticks_off()
    ticks = [1, 2, 4, 8, 16, 32]
    set_ticks(ticks)
    set_labels([str(tick) for tick in ticks] if labelled else [])


def trace_axis(axes, font_size=6):
    axes.set_xticks(range(9))
    axes.set_xticklabels(TRACES, fontsize=font_size)
    axes.set_xlabel("Trace")
    axes.grid(axis="x", visible=False)


def stacked_bars(axes, categories):
    """Bars stacked in the order given: (label, color, values per trace)."""
    bottom = [0] * 9
    for label, color, values in categories:
        axes.bar(range(9), values, bottom=bottom, color=color, label=label, width=0.75)
        bottom = [low + value for low, value in zip(bottom, values)]
    axes.set_ylim(0, 100)


def panel_title(axes, text):
    """Sub-figure caption at a fixed height, so that all panels line up."""
    box = axes.get_position()
    axes.figure.text((box.x0 + box.x1) / 2, 0.012, text, ha="center", va="bottom", fontsize=7.5)


def figure_a():
    """Synthetic workloads: speedup above, on the same x axis the share of attempts aborted."""
    groups = [[("a1", "Cores", None, "(a) ERC20, θ = 0.99"), ("a2", "Zipf skew θ (64 cores)", None, "(b) ERC20, skew"),
               ("a3", "Cores", None, "(c) DeFi-Mix, defaults")],
              [("a4", "Hot share h (%), 64 cores", "20", "(d) DeFi-Mix, hot share"),
               ("a5", "Write ratio w (%), 64 cores", "50", "(e) DeFi-Mix, writes"),
               ("a6", "First hot read p (%), 64 cores", "20", "(f) DeFi-Mix, position")]]
    rows = read("figa_synth")
    figure = plt.figure(figsize=(TEXT_WIDTH, 4.3))
    outer = GridSpec(2, 1, figure=figure, left=0.065, right=0.99, top=0.93, bottom=0.075, hspace=0.34)
    legend_axes = None
    for group_index, group in enumerate(groups):
        grid = outer[group_index].subgridspec(2, 3, wspace=0.12, hspace=0.1, height_ratios=[1.15, 1])
        for column, (panel, x_label, default, title) in enumerate(group):
            top = figure.add_subplot(grid[0, column])
            bottom = figure.add_subplot(grid[1, column], sharex=top)
            legend_axes = legend_axes or top
            points = [row for row in rows if row["col"] == panel]
            xs = list(dict.fromkeys(row["x"] for row in points))
            by_cores = x_label == "Cores"
            for system in SYSTEMS:
                curve = [row for row in points if row["system"] == system]
                positions = [int(row["x"]) if by_cores else xs.index(row["x"]) for row in curve]
                system_line(top, positions, [float(row["speedup"]) for row in curve], system, label=system, marker_size=3)
                system_line(bottom, positions, [100 * float(row["abort_rate"]) for row in curve], system, marker_size=3)
            if by_cores:
                cores_axis(bottom)
            else:
                bottom.set_xticks(range(len(xs)))
                bottom.set_xticklabels(xs)
                bottom.set_xlabel(x_label)
            if default in xs:
                for axes in (top, bottom):
                    axes.axvline(xs.index(default), color="#7a7a7a", lw=0.8, ls=":", zorder=1)
            top.tick_params(labelbottom=False)
            top.set_title(title, pad=3)
            speedup_scale(top.set_yscale, top.set_yticks, top.set_yticklabels, top, column == 0)
            top.set_ylim(0.6, 40)
            top.axhline(1, color=GRAY, lw=0.5, zorder=0)
            bottom.set_ylim(0, 100)
            bottom.set_yticks([0, 25, 50, 75, 100])
            if column == 0:
                top.set_ylabel("Speedup")
                bottom.set_ylabel("Aborted (%)")
            else:
                bottom.tick_params(labelleft=False)
    figure.legend(*legend_axes.get_legend_handles_labels(), loc="lower center", bbox_to_anchor=(0.5, 0.95),
                  ncol=3, frameon=False, columnspacing=1.4)
    return figure


def figure_b():
    """Composition of the traces, and where they lie against a sample of the chain."""
    figure, (composition, chain) = plt.subplots(1, 2, figsize=(COLUMN_WIDTH, 2.0), gridspec_kw={"width_ratios": [1, 1.05]})
    margins(figure, 0.13, 0.8, 0.25, 0.34)
    rows = {row["trace"]: row for row in read("figb_mix")}
    kinds = (("defi", "Multi-contract DeFi", "#0d4a80"), ("swap", "Single-pool swap", BLUE), ("nft", "NFT", YELLOW),
             ("token", "Token transfer", PALE_BLUE), ("eth", "ETH payment", "#e6f2fb"),
             ("single", "Other single contract", LIGHT_GRAY), ("failed", "Reverted", PALE_RED))
    stacked_bars(composition, [(label, color, [float(rows[trace][key]) for trace in TRACES]) for key, label, color in kinds])
    trace_axis(composition)
    composition.set_ylabel("Transactions (%)")
    figure.legend(*composition.get_legend_handles_labels(), loc="upper center", ncol=4, frameon=False, fontsize=5.6,
                  columnspacing=0.7, handlelength=1)
    sample = read("figb_sample")
    chain.scatter([float(row["event_width"]) for row in sample], [float(row["event_free_pct"]) for row in sample],
                  s=1.5, color=GRAY, alpha=0.3, lw=0, rasterized=True)
    for row in read("figb_traces"):
        x, y = float(row["exact_width"]), float(row["exact_free_pct"])
        chain.scatter([x], [y], s=9, color=BLUE, ec="black", lw=0.3, zorder=3)
        chain.annotate(row["trace"], (x, y), xytext=(2, 2), textcoords="offset points", fontsize=6)
    chain.set_xlim(0, 12)
    chain.set_ylim(0, 100)
    chain.set_xlabel("Dependency width")
    chain.set_ylabel("Conflict-free txs (%)")
    panel_title(composition, "(a) Composition")
    panel_title(chain, "(b) Against the chain")
    return figure


def figure_c():
    """Speedup by cores on the nine traces, with the limit without overhead."""
    rows = read("figc_curves")
    limits = read("figc_bound")
    widths = {row["trace"]: float(row["exact_width"]) for row in read("figb_traces")}
    figure, grid = plt.subplots(3, 3, figsize=(TEXT_WIDTH, 3.9), sharex=True, sharey=True)
    margins(figure, 0.06, 0.9, 0.095, 0.08, right=0.99)
    figure.subplots_adjust(hspace=0.24)
    for axes, trace in zip(grid.flat, TRACES):
        for system in SYSTEMS:
            curve = [row for row in rows if row["trace"] == trace and row["system"] == system]
            system_line(axes, [int(row["cores"]) for row in curve], [float(row["speedup"]) for row in curve],
                        system, label=system, marker_size=2.8)
        limit = [row for row in limits if row["trace"] == trace]
        axes.plot([int(row["cores"]) for row in limit], [float(row["limit"]) for row in limit],
                  color=SLATE, ls="--", lw=1, label="Limit without overhead", zorder=2)
        axes.set_title(f"Trace {trace}, width {widths[trace]:.1f}", pad=2)
    first = grid[0, 0]
    speedup_scale(first.set_yscale, first.set_yticks, first.set_yticklabels, first)
    first.set_ylim(0.6, 32)
    for axes in grid[:, 0]:
        axes.set_ylabel("Speedup")
    for axes in grid[2]:
        cores_axis(axes)
    figure.legend(*first.get_legend_handles_labels(), loc="upper center", ncol=4, frameon=False, columnspacing=1.4)
    return figure


def figure_d():
    """Aborts at 64 cores, and the distribution of the speedup over sliding windows of 16 blocks."""
    figure, (aborts, stability) = plt.subplots(1, 2, figsize=(COLUMN_WIDTH, 1.75))
    margins(figure, 0.135, 0.86, 0.27, 0.34)
    rows = {(row["trace"], row["system"]): row for row in read("figc_curves") if row["cores"] == "64"}
    for index, system in enumerate(SYSTEMS):
        aborts.bar([x + (index - 1) * 0.27 for x in range(9)],
                   [100 * float(rows[trace, system]["abort_rate"]) for trace in TRACES],
                   width=0.27, color=SERIES[system]["fill"], label=system)
    trace_axis(aborts)
    aborts.set_ylabel("Attempts aborted (%)")
    aborts.set_ylim(0, 100)
    rolling = read("figd_rolling")
    for system in SYSTEMS:
        values = sorted(float(row["speedup"]) for row in rolling if row["system"] == system)
        stability.step(values, [100 * (index + 1) / len(values) for index in range(len(values))],
                       where="post", color=SERIES[system]["color"], lw=1.1)
    speedup_scale(stability.set_xscale, stability.set_xticks, stability.set_xticklabels, stability)
    stability.set_xlim(0.7, 40)
    stability.set_ylim(0, 100)
    stability.set_xlabel("Speedup over 16 blocks")
    stability.set_ylabel("Windows (%)")
    figure.legend(*aborts.get_legend_handles_labels(), loc="upper center", ncol=3, frameon=False, fontsize=6.5)
    panel_title(aborts, "(a) Aborts")
    panel_title(stability, "(b) Stability")
    return figure


def figure_e():
    """The hottest conflict chain, and the critical region as a share of the transaction by trace."""
    figure, (gantt, region) = plt.subplots(1, 2, figsize=(COLUMN_WIDTH, 1.75), gridspec_kw={"width_ratios": [1.25, 1]})
    margins(figure, 0.1, 0.88, 0.25, 0.34)
    rows = read("fige_gantt")
    order = sorted({int(row["row"]) for row in rows})
    position = {row: index for index, row in enumerate(order)}
    markers = {"first": dict(marker="|", color=SLATE, ms=5, mew=1), "release": dict(marker="v", color=BLUE, ms=2.8),
               "commit": dict(marker="x", color=RED, ms=2.8, mew=0.7)}
    for segment, color, zorder in (("run", PALE_BLUE, 2), ("block", YELLOW, 3)):
        for row in (row for row in rows if row["seg"] == segment):
            start = float(row["t0_us"]) / 1000
            gantt.barh(position[int(row["row"])], float(row["t1_us"]) / 1000 - start, left=start, height=0.62,
                       color=color, lw=0.3, zorder=zorder)
    for row in rows:
        if row["seg"] in markers:
            gantt.plot([float(row["t0_us"]) / 1000], [position[int(row["row"])]], zorder=4, **markers[row["seg"]])
    gantt.set_yticks(range(len(order)))
    gantt.set_yticklabels([str(index + 1) for index in range(len(order))])
    gantt.invert_yaxis()
    gantt.set_xlabel("Time (ms)")
    gantt.set_ylabel("Transaction in chain")
    gantt.grid(axis="y", visible=False)
    by_trace = {row["trace"]: row for row in read("fige_region")}
    statistics = [dict(label=trace, whislo=float(by_trace[trace]["q05"]), q1=float(by_trace[trace]["q25"]),
                       med=float(by_trace[trace]["q50"]), q3=float(by_trace[trace]["q75"]),
                       whishi=float(by_trace[trace]["q95"]), fliers=[]) for trace in TRACES]
    region.bxp(statistics, showfliers=False, widths=0.6, patch_artist=True, boxprops=dict(facecolor=PALE_BLUE, lw=0.4),
               medianprops=dict(color=BLUE, lw=1), whiskerprops=dict(lw=0.4), capprops=dict(lw=0.4))
    region.set_ylim(0, 100)
    region.set_ylabel("Region / transaction (%)")
    region.set_xlabel("Trace")
    region.tick_params(axis="x", labelsize=6)
    figure.legend(handles=[Patch(fc=PALE_BLUE, label="Run"), Patch(fc=YELLOW, label="Blocked"),
                           *(Line2D([], [], ls="", label=label, **markers[key])
                             for key, label in (("first", "First access"), ("release", "Release"), ("commit", "Commit")))],
                  loc="upper center", ncol=5, frameon=False, fontsize=6, columnspacing=0.8, handlelength=1, handletextpad=0.3)
    panel_title(gantt, "(a) Hottest chain")
    panel_title(region, "(b) Region length")
    return figure


def figure_f():
    """Per-key decisions at the fence, with the mean per bin of 0.1, and their outcomes by trace."""
    figure, (keys, outcomes) = plt.subplots(1, 2, figsize=(COLUMN_WIDTH, 1.85), gridspec_kw={"width_ratios": [1, 1.15]})
    margins(figure, 0.13, 0.83, 0.27, 0.34)
    rows = read("figf_keys")
    written = [float(row["p_written"]) for row in rows]
    waited = [float(row["wait_pct"]) for row in rows]
    keys.scatter(written, waited, s=1.5, color=BLUE, alpha=0.2, lw=0, rasterized=True)
    bins = {}
    for x, y in zip(written, waited):
        bins.setdefault(min(9, int(x * 10)), []).append(y)
    keys.plot([(index + 0.5) / 10 for index in sorted(bins)], [sum(bins[index]) / len(bins[index]) for index in sorted(bins)],
              color="black", lw=1, marker="o", ms=2)
    keys.set_xlim(0, 1)
    keys.set_ylim(0, 100)
    keys.set_xlabel("Named writer wrote (share)")
    keys.set_ylabel("Waited (%)")
    rows = {row["trace"]: {key: int(value) for key, value in row.items() if key != "trace"} for row in read("figf_decisions")}
    for row in rows.values():
        row["spec_wrong"] = row.pop("spec_wrong_named") + row.pop("spec_wrong_unnamed")
    categories = (("wait_right", "Wait, right", YELLOW), ("spec_right", "Speculate, right", PALE_BLUE),
                  ("wait_vain", "Wait, in vain", LIGHT_GRAY), ("waited_then_aborted", "Wait, then aborted", PALE_RED),
                  ("spec_wrong", "Speculate, wrong", RED))
    stacked_bars(outcomes, [(label, color, [100 * rows[trace][key] / sum(rows[trace].values()) for trace in TRACES])
                            for key, label, color in categories])
    trace_axis(outcomes)
    outcomes.set_ylabel("Decisions (%)")
    figure.legend(*outcomes.get_legend_handles_labels(), loc="upper center", ncol=3, frameon=False, fontsize=5.8,
                  columnspacing=0.7, handlelength=1)
    panel_title(keys, "(a) Per-key decision")
    panel_title(outcomes, "(b) Outcomes")
    return figure


def figure_g():
    """Effect of learning, writer hit rate over the 900 blocks (mean of 10), and the window width over
    the whole run."""
    figure, (effect, hits, width) = plt.subplots(1, 3, figsize=(TEXT_WIDTH, 1.7))
    margins(figure, 0.062, 0.96, 0.27, 0.24, right=0.982)
    rows = {(row["trace"], row["variant"]): row for row in read("figg_effect")}
    variants = (("default", "Learned, all", BLUE), ("no_cross_block", "No cross-block", SKY),
                ("no_learned_waits", "No waits on learned", LIGHT_GRAY))
    for index, (variant, label, color) in enumerate(variants):
        effect.bar([x + (index - 1) * 0.27 for x in range(9)], [100 * float(rows[trace, variant]["abort_rate"]) for trace in TRACES],
                   width=0.27, color=color, label=label)
    trace_axis(effect, font_size=7)
    effect.set_ylim(0, 75)
    effect.set_ylabel("Attempts aborted (%)")
    effect.legend(loc="upper left", fontsize=6, ncol=1)
    rows = read("figg_hits")
    xs, ys = [], []
    for start in range(891):
        window = rows[start:start + 10]
        waits = sum(int(row["waits"]) for row in window)
        if waits:
            xs.append(start + 5)
            ys.append(100 * sum(int(row["hits"]) for row in window) / waits)
    hits.plot(xs, ys, color=BLUE, lw=0.9)
    for index, trace in enumerate(TRACES):
        if index:
            hits.axvline(100 * index, color=GRAY, lw=0.5, ls=":")
        hits.text(100 * index + 50, 3, trace, ha="center", fontsize=6.5, color=GRAY)
    hits.set_xlim(0, 900)
    hits.set_ylim(0, 100)
    hits.set_xticks([0, 300, 600, 900])
    hits.set_xlabel("Blocks so far")
    hits.set_ylabel("Writer hit rate (%)")
    rows = read("figg_width")
    width.plot([100 * TRACES.index(row["trace"]) + int(row["block_idx"]) for row in rows],
               [int(row["width"]) for row in rows], color=BLUE, lw=0.8)
    for index, trace in enumerate(TRACES):
        if index:
            width.axvline(100 * index, color=GRAY, lw=0.5, ls=":")
        width.text(100 * index + 50, 36.5, trace, ha="center", fontsize=6.5, color=GRAY)
    width.set_xlim(0, 900)
    width.set_ylim(0, 40)
    width.set_xticks([0, 300, 600, 900])
    width.set_xlabel("Blocks so far")
    width.set_ylabel("Window width (blocks)")
    panel_title(effect, "(a) Effect of learning")
    panel_title(hits, "(b) Across blocks")
    panel_title(width, "(c) Window width")
    return figure


def main():
    for letter in sys.argv[1:] or "abcdefg":
        figure = globals()[f"figure_{letter}"]()
        figure.savefig(HERE / f"figure_{letter}.pdf")
        plt.close(figure)


if __name__ == "__main__":
    main()
