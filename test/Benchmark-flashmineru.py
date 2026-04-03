#!/usr/bin/env python3
"""
Benchmark Flash-MinerU on a folder of PDFs (run from repo root; Ray + vLLM required).

Default input is ``test/sample_pdfs``. Override with ``--pdf-dir``.

Uses :class:`flash_mineru.MineruEngineLegacy` (v0.0.4 sequential batching). The default library API is
:class:`flash_mineru.MineruEngine` (v1.0.0); see ``Benchmark-flashmineru_dag.py`` for that path.

Use ``--batch-size`` >= replica count so one batch can fan out across GPUs (default 16).

Example
-------
  python -u test/Benchmark-flashmineru.py --model /path/to/MinerU2.5-2509-1.2B

  python -u test/Benchmark-flashmineru.py --model ... --replicas 8 --batch-size 16

  python -u test/Benchmark-flashmineru.py --model ... --smoke

  python -u test/Benchmark-flashmineru.py --model ... --profile

A JSON report is written under ``--save-dir/reports/`` (or ``--report``). After ``engine.run()``,
``<save-dir>/profile.json`` may be written via ``ray.timeline`` for chrome://tracing / Perfetto.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path
from shlex import join as shlex_join

# Ensure package import when run as script from repo root or test/
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from benchmark_utils import (
    apply_flash_smoke_overrides,
    collect_pdfs_for_benchmark,
    log,
    namespace_to_config,
    repo_test_dir,
    save_benchmark_report,
    wall_seconds,
    print_summary,
)


def _effective_max_batches(args: argparse.Namespace) -> int | None:
    if args.profile and args.max_batches is None:
        return 3
    return args.max_batches


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Benchmark Flash-MinerU MineruEngineLegacy (v0.0.4 sequential) throughput."
    )
    default_pdf = repo_test_dir() / "sample_pdfs"
    p.add_argument(
        "--pdf-dir",
        type=Path,
        default=default_pdf,
        help=f"Directory containing PDFs (default: repo test/sample_pdfs: {default_pdf})",
    )
    p.add_argument(
        "--model",
        default=os.environ.get("FLASH_MINERU_MODEL", ""),
        help="VLM model path (or set env FLASH_MINERU_MODEL)",
    )
    p.add_argument(
        "--save-dir",
        type=Path,
        default=repo_test_dir() / "benchmark_outputs" / "flash_mineru",
        help="Output directory for parsed artifacts",
    )
    p.add_argument("--replicas", type=int, default=8, help="Ray replicas (GPU workers)")
    p.add_argument(
        "--num-gpus-per-replica",
        type=float,
        default=1.0,
        help="Fractional GPUs per replica (see MineruEngineLegacy)",
    )
    p.add_argument(
        "--batch-size",
        type=int,
        default=16,
        metavar="N",
        help="PDFs per run batch; use >= num replicas (default 16) to use multiple GPUs per batch and cut RPC rounds",
    )
    p.add_argument(
        "--engine-gpu-util",
        type=float,
        default=0.9,
        help="engine_gpu_util_rate_to_ray_cap passed to MineruEngineLegacy",
    )
    p.add_argument(
        "--report",
        type=Path,
        default=None,
        help="Write JSON report here; default: <save-dir>/reports/flash_mineru_<timestamp>.json",
    )
    p.add_argument(
        "--max-batches",
        type=int,
        default=None,
        metavar="N",
        help="Process at most N batches (first N * batch-size PDFs). Omit to run all PDFs.",
    )
    p.add_argument(
        "--profile",
        action="store_true",
        help="Shorthand: --max-batches 3 (quick multi-batch probe).",
    )
    p.add_argument(
        "--smoke",
        action="store_true",
        help=(
            "Single-PDF smoke: replicas=1, batch-size=1, max-batches=1, first PDF only "
            "(or auto-create smoke_minimal.pdf if the directory has no PDFs)."
        ),
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    apply_flash_smoke_overrides(args)
    if not args.smoke and args.batch_size < 8:
        log("ERROR: --batch-size must be >= 8 so batches are large enough to fan out across replicas (try 16).")
        return 2
    if not args.model or not str(args.model).strip():
        log("ERROR: pass --model /path/to/MinerU2.5 or set FLASH_MINERU_MODEL")
        return 2

    max_batches = _effective_max_batches(args)

    pdf_dir = args.pdf_dir.resolve()
    pdfs = collect_pdfs_for_benchmark(pdf_dir, smoke=args.smoke)
    paths = [str(p) for p in pdfs]
    log(f"Found {len(paths)} PDFs under {pdf_dir}")
    if max_batches is not None:
        cap = max_batches * args.batch_size
        if len(paths) > cap:
            paths = paths[:cap]
        log(
            f"Capped to {len(paths)} PDFs (~{max_batches} batch(es) at batch_size={args.batch_size})."
        )

    from flash_mineru import MineruEngineLegacy

    save_dir = str(args.save_dir.resolve())
    os.makedirs(save_dir, exist_ok=True)

    t0 = time.perf_counter()
    engine = MineruEngineLegacy(
        model=str(Path(args.model).expanduser().resolve()),
        batch_size=args.batch_size,
        replicas=args.replicas,
        num_gpus_per_replica=args.num_gpus_per_replica,
        save_dir=save_dir,
        engine_gpu_util_rate_to_ray_cap=args.engine_gpu_util,
    )
    results = engine.run(paths)
    elapsed = wall_seconds(t0)

    profile_path = Path(save_dir).resolve() / "profile.json"
    ray_timeline_written: str | None = None
    try:
        import ray

        ray.timeline(filename=str(profile_path))
        ray_timeline_written = str(profile_path)
        log(f"Ray timeline written: {profile_path}")
    except Exception as e:
        log(f"WARNING: ray.timeline failed: {e}")

    result_payload = {
        "tool": "flash_mineru",
        "num_pdfs": len(paths),
        "pdf_dir": str(pdf_dir),
        "model": str(args.model),
        "replicas": args.replicas,
        "num_gpus_per_replica": args.num_gpus_per_replica,
        "batch_size": args.batch_size,
        "save_dir": save_dir,
        "wall_seconds": round(elapsed, 3),
        "result_batches": len(results) if results is not None else 0,
        "exit_code": 0,
        "max_batches_limit": max_batches,
        "ray_timeline_path": ray_timeline_written,
    }

    config = namespace_to_config(args)
    config["effective_max_batches"] = max_batches
    config["pdf_dir_resolved"] = str(pdf_dir)
    config["model_resolved"] = str(Path(args.model).expanduser().resolve())
    config["save_dir_resolved"] = save_dir
    config["invocation"] = shlex_join(sys.argv)

    report_path = save_benchmark_report(
        config,
        result_payload,
        report_path=args.report,
        default_dir=Path(save_dir) / "reports",
        filename_prefix="flash_mineru",
    )
    log(f"Report saved: {report_path}")

    print_summary({"config": config, "results": result_payload})
    return 0


if __name__ == "__main__":
    sys.exit(main())
