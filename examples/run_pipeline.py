"""Run any bundled Flash-MinerU pipeline with one small command-line interface."""

from __future__ import annotations

import argparse
from pathlib import Path

from flash_mineru import MineruEngine, pipeline_names


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pipeline", required=True, choices=pipeline_names())
    parser.add_argument(
        "--model",
        required=True,
        help="MinerU 2.5 checkpoint or MinerU 4 model root",
    )
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--gpus", type=int, default=1)
    parser.add_argument("--vlm-server-url", default="")
    parser.add_argument("--vlm-model", default="")
    return parser.parse_args()


def collect_pdfs(path: Path) -> list[str]:
    pdfs = [path] if path.is_file() else sorted(path.glob("*.pdf"))
    if not pdfs:
        raise SystemExit(f"no PDFs found at {path}")
    return [str(pdf.resolve()) for pdf in pdfs]


def main() -> None:
    args = parse_args()
    if args.gpus < 1:
        raise SystemExit("--gpus must be at least 1")

    with MineruEngine(
        pipeline_version=args.pipeline,
        model=args.model,
        save_dir=str(args.output),
        replicas=args.gpus,
        num_gpus_per_replica=1.0,
        batch_size=16,
        input_batch_size=24,
        inflight=4,
        vlm_server_url=args.vlm_server_url,
        vlm_model=args.vlm_model,
    ) as engine:
        batches = engine.run(collect_pdfs(args.input))

    for batch in batches:
        for output in batch:
            print(output)


if __name__ == "__main__":
    main()
