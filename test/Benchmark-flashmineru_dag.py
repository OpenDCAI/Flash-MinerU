#!/usr/bin/env python3
"""
Benchmark Flash-MinerU default :class:`flash_mineru.MineruEngine` (v1.0.0 pipeline parallelism).

Default PDFs: ``test/sample_pdfs``. Same discovery, caps, reporting, and ``profile.json`` timeline as
``Benchmark-flashmineru.py``. Uses the same public API as ``from flash_mineru import MineruEngine``
(RayOrch ``DagExecutor`` + overlapped batches under ``flash_mineru.ray_utils.rayorch_runtime``).

Example
-------
  python -u test/Benchmark-flashmineru_dag.py \\
    --model /path/to/MinerU2.5-2509-1.2B --replicas 8 --inflight 4

  python -u test/Benchmark-flashmineru_dag.py --model ... --profile

  python -u test/Benchmark-flashmineru_dag.py --model ... --smoke
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path
from shlex import join as shlex_join

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
        description="Benchmark default MineruEngine (v1.0.0 pipeline-parallel path)."
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
        default=repo_test_dir() / "benchmark_outputs" / "flash_mineru_dag",
        help="Output directory for parsed artifacts",
    )
    p.add_argument(
        "--replicas",
        type=int,
        default=8,
        help="Replica count for pdf2img, process_img, and img2md (each stage uses SHARD_CONTIGUOUS / same fan-out)",
    )
    p.add_argument(
        "--num-gpus-per-replica",
        type=float,
        default=1.0,
        help="Fractional GPUs per replica",
    )
    p.add_argument(
        "--batch-size",
        type=int,
        default=16,
        metavar="N",
        help="PDFs per logical batch (same as MineruEngine / MineruEngineLegacy)",
    )
    p.add_argument(
        "--engine-gpu-util",
        type=float,
        default=0.9,
        help="Passed through as engine_gpu_util_rate_to_ray_cap scaling",
    )
    p.add_argument(
        "--inflight",
        type=int,
        default=4,
        metavar="N",
        help=(
            "Single cap: DagExecutor max overlapped batches and max_inflight on pdf2img / "
            "process_img / img2md stages"
        ),
    )
    p.add_argument(
        "--report",
        type=Path,
        default=None,
        help="Write JSON report here; default: <save-dir>/reports/flash_mineru_dag_<ts>.json",
    )
    p.add_argument(
        "--max-batches",
        type=int,
        default=None,
        metavar="N",
        help="Process at most N logical batches. Omit to run all PDFs.",
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
            "Single-PDF smoke: replicas=1, batch-size=1, inflight=1, max-batches=1, first PDF only "
            "(or auto-create smoke_minimal.pdf if the directory has no PDFs)."
        ),
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    apply_flash_smoke_overrides(args)
    if not args.smoke and args.batch_size < 8:
        log("ERROR: --batch-size must be >= 8 (try 16).")
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

    try:
        from flash_mineru import MineruEngine
    except ImportError as e:
        log(f"ERROR: MineruEngine import failed: {e}")
        log(
            "Check that flash_mineru.ray_utils.rayorch_runtime is present and dependencies (ray, torch) are installed."
        )
        return 2

    save_dir = str(args.save_dir.resolve())
    os.makedirs(save_dir, exist_ok=True)

    log(
        "Building MineruEngine (Ray actors + vLLM load); "
        "this can be silent for several minutes — use python -u (or unbuffered stdout) for live logs."
    )
    t0 = time.perf_counter()
    engine = MineruEngine(
        model=str(Path(args.model).expanduser().resolve()),
        batch_size=args.batch_size,
        replicas=args.replicas,
        num_gpus_per_replica=args.num_gpus_per_replica,
        save_dir=save_dir,
        engine_gpu_util_rate_to_ray_cap=args.engine_gpu_util,
        inflight=args.inflight,
        dev_mode=False,
    )
    log("Engine ready; running batches...")
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
        "tool": "flash_mineru_rayorch_dag",
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
        "inflight": args.inflight,
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
        filename_prefix="flash_mineru_dag",
    )
    log(f"Report saved: {report_path}")

    print_summary({"config": config, "results": result_payload})
    return 0


if __name__ == "__main__":
    sys.exit(main())
