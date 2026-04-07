import argparse
import glob
import json
import os
from collections import defaultdict
from typing import Dict, List, Tuple

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


DEFAULT_ARTIFACTS_DIR = "artifacts"
DEFAULT_OUTPUT_DIR = "artifacts/plots"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build comparison charts from Spark experiment artifacts."
    )
    parser.add_argument(
        "--artifacts-dir",
        default=DEFAULT_ARTIFACTS_DIR,
        help="Directory with experiment JSON artifacts.",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help="Directory where charts will be saved.",
    )
    parser.add_argument(
        "--use-all-runs",
        action="store_true",
        help="Use all artifacts instead of only the latest run for each experiment type.",
    )
    return parser.parse_args()


def ensure_output_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def load_artifacts(artifacts_dir: str) -> List[Dict]:
    paths = sorted(glob.glob(os.path.join(artifacts_dir, "experiment*.json")))
    artifacts = []
    for path in paths:
        with open(path, "r", encoding="utf-8") as file:
            payload = json.load(file)
        payload["_path"] = path
        artifacts.append(payload)
    return artifacts


def infer_experiment_label(app_name: str) -> str:
    app_name_lower = app_name.lower()
    node_label = "3DN" if "3dn" in app_name_lower else "1DN"
    mode_label = "Optimized" if "optimized" in app_name_lower else "Baseline"
    return f"{node_label} {mode_label}"


def select_latest_per_experiment(artifacts: List[Dict]) -> List[Dict]:
    latest: Dict[str, Dict] = {}
    for artifact in artifacts:
        label = infer_experiment_label(artifact["app_name"])
        prev = latest.get(label)
        if prev is None or artifact["experiment_timestamp"] > prev["experiment_timestamp"]:
            latest[label] = artifact
    order = ["1DN Baseline", "1DN Optimized", "3DN Baseline", "3DN Optimized"]
    return [latest[label] for label in order if label in latest]


def build_step_duration_map(artifacts: List[Dict]) -> Tuple[List[str], Dict[str, List[float]]]:
    all_steps = []
    for artifact in artifacts:
        for step in artifact.get("steps", []):
            if step["step"] not in all_steps:
                all_steps.append(step["step"])

    values: Dict[str, List[float]] = defaultdict(list)
    for artifact in artifacts:
        step_map = {step["step"]: step["duration_sec"] for step in artifact.get("steps", [])}
        for step_name in all_steps:
            values[step_name].append(step_map.get(step_name, 0.0))
    return all_steps, values


def get_parallelism_config(artifact: Dict) -> Tuple[int, int]:
    spark_config = artifact.get("spark_config", {})
    shuffle_partitions = int(spark_config.get("shuffle_partitions", 0) or 0)
    repartition_partitions = int(spark_config.get("target_partitions", 0) or 0)
    return shuffle_partitions, repartition_partitions


def build_parallelism_note(artifact: Dict) -> str:
    shuffle_partitions, repartition_partitions = get_parallelism_config(artifact)
    repartition_label = repartition_partitions if repartition_partitions > 0 else "-"
    return f"shuffle={shuffle_partitions}\nrepartition={repartition_label}"


def save_total_duration_chart(artifacts: List[Dict], output_dir: str) -> str:
    labels = [infer_experiment_label(a["app_name"]) for a in artifacts]
    values = [a["total_duration_sec"] for a in artifacts]

    plt.figure(figsize=(10, 6))
    bars = plt.bar(labels, values, color=["#4e79a7", "#59a14f", "#f28e2b", "#e15759"][: len(labels)])
    plt.title("Total Experiment Duration")
    plt.ylabel("Seconds")
    plt.xticks(rotation=15)
    plt.grid(axis="y", linestyle="--", alpha=0.4)

    for bar, artifact, value in zip(bars, artifacts, values):
        plt.text(bar.get_x() + bar.get_width() / 2, value, f"{value:.1f}", ha="center", va="bottom")
        plt.text(
            bar.get_x() + bar.get_width() / 2,
            value * 0.55,
            build_parallelism_note(artifact),
            ha="center",
            va="center",
            fontsize=8,
            color="black",
        )

    output_path = os.path.join(output_dir, "total_duration.png")
    plt.tight_layout()
    plt.savefig(output_path, dpi=160)
    plt.close()
    return output_path


def save_step_duration_chart(artifacts: List[Dict], output_dir: str) -> str:
    labels = [infer_experiment_label(a["app_name"]) for a in artifacts]
    step_names, step_values = build_step_duration_map(artifacts)

    x_positions = range(len(labels))
    bottom = [0.0] * len(labels)
    colors = ["#4e79a7", "#59a14f", "#f28e2b", "#e15759", "#76b7b2"]

    plt.figure(figsize=(11, 7))
    for index, step_name in enumerate(step_names):
        values = step_values[step_name]
        plt.bar(
            x_positions,
            values,
            bottom=bottom,
            label=step_name,
            color=colors[index % len(colors)],
        )
        bottom = [current + value for current, value in zip(bottom, values)]

    plt.title("Step Duration Breakdown")
    plt.ylabel("Seconds")
    plt.xticks(list(x_positions), labels, rotation=15)
    plt.grid(axis="y", linestyle="--", alpha=0.4)
    plt.legend()

    total_heights = [sum(step_values[step_name][index] for step_name in step_names) for index in range(len(labels))]
    for index, artifact in enumerate(artifacts):
        plt.text(
            index,
            total_heights[index],
            build_parallelism_note(artifact),
            ha="center",
            va="bottom",
            fontsize=8,
        )

    output_path = os.path.join(output_dir, "step_duration_breakdown.png")
    plt.tight_layout()
    plt.savefig(output_path, dpi=160)
    plt.close()
    return output_path


def save_rows_chart(artifacts: List[Dict], output_dir: str) -> str:
    labels = [infer_experiment_label(a["app_name"]) for a in artifacts]
    values = [a["result_metrics"]["total_rows_after_cleanup"] for a in artifacts]

    plt.figure(figsize=(10, 6))
    bars = plt.bar(labels, values, color="#edc948")
    plt.title("Rows After Cleanup")
    plt.ylabel("Rows")
    plt.xticks(rotation=15)
    plt.grid(axis="y", linestyle="--", alpha=0.4)

    for bar, value in zip(bars, values):
        plt.text(bar.get_x() + bar.get_width() / 2, value, f"{value:,}", ha="center", va="bottom")

    output_path = os.path.join(output_dir, "rows_after_cleanup.png")
    plt.tight_layout()
    plt.savefig(output_path, dpi=160)
    plt.close()
    return output_path

def save_summary_table(artifacts: List[Dict], output_dir: str) -> str:
    output_path = os.path.join(output_dir, "summary.tsv")
    with open(output_path, "w", encoding="utf-8") as file:
        file.write(
            "label\tapp_name\ttotal_duration_sec\tpeak_driver_ram_mb\tshuffle_partitions\ttarget_partitions\tartifact\n"
        )
        for artifact in artifacts:
            spark_config = artifact.get("spark_config", {})
            file.write(
                "\t".join(
                    [
                        infer_experiment_label(artifact["app_name"]),
                        artifact["app_name"],
                        str(artifact.get("total_duration_sec", "")),
                        str(artifact.get("peak_driver_ram_mb", "")),
                        str(spark_config.get("shuffle_partitions", "")),
                        str(spark_config.get("target_partitions", "")),
                        artifact["_path"],
                    ]
                )
                + "\n"
            )
    return output_path


def main() -> None:
    args = parse_args()
    ensure_output_dir(args.output_dir)

    artifacts = load_artifacts(args.artifacts_dir)
    if not artifacts:
        raise SystemExit(f"No experiment JSON files found in {args.artifacts_dir}")

    selected_artifacts = artifacts if args.use_all_runs else select_latest_per_experiment(artifacts)

    generated_files = [
        save_total_duration_chart(selected_artifacts, args.output_dir),
        save_step_duration_chart(selected_artifacts, args.output_dir),
        save_rows_chart(selected_artifacts, args.output_dir),
        save_summary_table(selected_artifacts, args.output_dir),
    ]

    print("Generated files:")
    for path in generated_files:
        print(path)


if __name__ == "__main__":
    main()
