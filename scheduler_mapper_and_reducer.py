#!/usr/bin/env python3
"""
Production-ready weekly inverted-index batch job scheduler.

Features:
- Configurable via CLI flags, environment vars, or JSON config file
- Idempotent output handling (abort or archive)
- Validates Hadoop Streaming JAR path
- Retries with exponential backoff
- Structured logging with timestamps
- Stub for alerting on final failure
"""

import os
import sys
import json
import time
import argparse
import logging
import subprocess
from datetime import datetime

# -----------------------------------------------------------------------------
# Helper functions
# -----------------------------------------------------------------------------


def send_alert(message: str) -> None:
    """
    Stub for alert integration (Slack, PagerDuty, email, etc.).
    """
    logging.error("ALERT: %s", message)
    # TODO: integrate with real notification service


def load_json_config(path: str) -> dict:
    if not os.path.isfile(path):
        logging.error("Config file not found: %s", path)
        sys.exit(1)
    with open(path, "r") as f:
        return json.load(f)


def hdfs_path_exists(path: str) -> bool:
    """Return True if HDFS directory exists."""
    return (
        subprocess.run(
            ["hdfs", "dfs", "-test", "-d", path],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        ).returncode
        == 0
    )


def archive_hdfs_path(src: str, dest: str) -> None:
    """Move existing HDFS directory to an archive location."""
    logging.info("Archiving existing output: %s -> %s", src, dest)
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", os.path.dirname(dest)], check=True)
    subprocess.run(["hdfs", "dfs", "-mv", src, dest], check=True)


def run_with_retries(cmd: list, retries: int, backoff: int) -> int:
    """
    Run subprocess.run(cmd) up to `retries` times with exponential backoff.
    Returns 0 on success, 1 on final failure.
    """
    for attempt in range(1, retries + 1):
        start_time = time.time()
        try:
            logging.info("Launching Hadoop job (attempt %d/%d)...", attempt, retries)
            subprocess.run(cmd, check=True)
            duration = time.time() - start_time
            logging.info("Job succeeded in %.1f seconds", duration)
            return 0
        except subprocess.CalledProcessError as e:
            logging.warning("Job failed on attempt %d: %s", attempt, e)
            if attempt < retries:
                sleep_time = backoff * (2 ** (attempt - 1))
                logging.info("Sleeping %d seconds before retry", sleep_time)
                time.sleep(sleep_time)
            else:
                logging.error("All %d attempts failed", retries)
                send_alert(f"Hadoop job failed after {retries} attempts")
                return 1


# -----------------------------------------------------------------------------
# Main entrypoint
# -----------------------------------------------------------------------------


def main():
    # -------------------------------------------------------------------------
    # 1) Argument parsing
    # -------------------------------------------------------------------------
    parser = argparse.ArgumentParser(
        description="Weekly inverted-index batch job scheduler"
    )
    parser.add_argument("--config", help="Path to JSON config file")
    parser.add_argument("--input-path", default=os.getenv("INPUT_PATH", "/data/docs/"))
    parser.add_argument(
        "--output-base", default=os.getenv("OUTPUT_BASE", "/indexes/inverted/")
    )
    parser.add_argument(
        "--hadoop-jar",
        default=os.getenv(
            "HADOOP_STREAMING_JAR", "/usr/lib/hadoop-mapreduce/hadoop-streaming.jar"
        ),
    )
    parser.add_argument(
        "--num-reducers", type=int, default=int(os.getenv("NUM_REDUCERS", "26"))
    )
    parser.add_argument("--queue", default=os.getenv("YARN_QUEUE", "default"))
    parser.add_argument(
        "--map-memory", type=int, default=int(os.getenv("MAP_MEMORY_MB", "4096"))
    )
    parser.add_argument(
        "--reduce-memory", type=int, default=int(os.getenv("REDUCE_MEMORY_MB", "8192"))
    )
    parser.add_argument("--retries", type=int, default=int(os.getenv("RETRIES", "3")))
    parser.add_argument(
        "--backoff", type=int, default=int(os.getenv("RETRY_BACKOFF", "60"))
    )
    parser.add_argument(
        "--archive",
        action="store_true",
        help="Archive existing output directory instead of aborting",
    )
    args = parser.parse_args()

    # -------------------------------------------------------------------------
    # 2) Load & override from JSON config if provided
    # -------------------------------------------------------------------------
    cfg = {}
    if args.config:
        cfg = load_json_config(args.config)

    input_path = cfg.get("input_path", args.input_path)
    output_base = cfg.get("output_base", args.output_base)
    hadoop_jar = cfg.get("hadoop_jar", args.hadoop_jar)
    num_reducers = cfg.get("num_reducers", args.num_reducers)
    queue = cfg.get("queue", args.queue)
    map_memory = cfg.get("map_memory", args.map_memory)
    reduce_memory = cfg.get("reduce_memory", args.reduce_memory)
    retries = cfg.get("retries", args.retries)
    backoff = cfg.get("backoff", args.backoff)
    archive_flag = cfg.get("archive", args.archive)

    # -------------------------------------------------------------------------
    # 3) Configure logging
    # -------------------------------------------------------------------------
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )

    # -------------------------------------------------------------------------
    # 4) Pre-flight validations
    # -------------------------------------------------------------------------
    if not os.path.isfile(hadoop_jar):
        logging.error("Hadoop Streaming JAR not found at %s", hadoop_jar)
        sys.exit(1)

    today = datetime.utcnow().strftime("%Y%m%d")
    output = output_base.rstrip("/") + f"/{today}"

    if hdfs_path_exists(output):
        if archive_flag:
            ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
            archive_dest = output_base.rstrip("/") + f"/archive/{today}_{ts}"
            archive_hdfs_path(output, archive_dest)
        else:
            logging.error("Output path %s already exists; aborting", output)
            sys.exit(1)

    # -------------------------------------------------------------------------
    # 5) Build Hadoop Streaming command
    # -------------------------------------------------------------------------
    cmd = [
        "hadoop",
        "jar",
        hadoop_jar,
        "-D",
        f"mapreduce.job.name=inverted_index_{today}",
        "-D",
        f"mapreduce.job.reduces={num_reducers}",
        "-D",
        f"mapreduce.map.memory.mb={map_memory}",
        "-D",
        f"mapreduce.reduce.memory.mb={reduce_memory}",
        "-D",
        f"yarn.queue.name={queue}",
        "-input",
        input_path,
        "-output",
        output,
        "-mapper",
        "python3 mapper.py",
        "-reducer",
        "python3 reducer.py",
        "-partitioner",
        "com.example.FirstLetterPartitioner",
    ]

    logging.info("Running MapReduce job: %s", " ".join(cmd))

    # -------------------------------------------------------------------------
    # 6) Execute with retries
    # -------------------------------------------------------------------------
    exit_code = run_with_retries(cmd, retries, backoff)
    if exit_code == 0:
        logging.info(
            "Batch inverted-index run completed successfully. Output at: %s", output
        )
    else:
        logging.error("Batch job failed after retries; see logs for details.")
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
