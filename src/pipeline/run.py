"""Generate the sample feed and load it.

    python -m src.pipeline.run --data data/sample --warehouse warehouse.duckdb
"""

from __future__ import annotations

import argparse
from pathlib import Path

from src.datagen.generator import generate
from src.pipeline.pipeline import Pipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate the sample batches and run the pipeline over them")
    parser.add_argument("--data", default="data/sample")
    parser.add_argument("--warehouse", default="warehouse.duckdb")
    parser.add_argument("--fault", action="append", default=[], help="inject a known defect (see KNOWN_FAULTS)")
    args = parser.parse_args()
    Path(args.warehouse).unlink(missing_ok=True)
    batches = generate(Path(args.data))
    pipeline = Pipeline(args.warehouse, faults=set(args.fault))
    for batch_id in batches:
        print(batch_id, pipeline.run_batch(Path(args.data) / batch_id))
    pipeline.close()


if __name__ == "__main__":
    main()
