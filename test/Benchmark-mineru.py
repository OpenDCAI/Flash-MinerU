#!/usr/bin/env python3
"""
Benchmark upstream MinerU on a folder of PDFs (``mineru`` must be on PATH or next to this Python).

Default input: ``test/sample_pdfs``. Official CLI: https://github.com/opendatalab/MinerU

Modes
-----
single    One ``mineru -p <pdf_dir> -o <out>`` process.
parallel  Split PDFs into N shards; one ``mineru`` per non-empty shard with
          ``CUDA_VISIBLE_DEVICES=<shard_index>``. Prefer ``--vlm-model-dir`` so shards do not
          each download from HuggingFace.

Example
-------
  python -u test/Benchmark-mineru.py --mode parallel --num-shards 8
  python -u test/Benchmark-mineru.py --mode single

  python -u test/Benchmark-mineru.py --keep-nvidia-visible --mode parallel --num-shards 8

  python -u test/Benchmark-mineru.py --smoke --vlm-model-dir /path/to/MinerU2.5-2509-1.2B

  python -u test/debug_mineru_gpu.py

  python -u test/Benchmark-mineru.py --vlm-model-dir /path/to/MinerU2.5-2509-1.2B
  # Or: export MINERU_VLM_MODEL_DIR=/path/to/MinerU2.5-2509-1.2B

JSON report: ``--output-dir/reports/`` unless ``--report``. Cleanup uses process groups plus an
optional psutil sweep of leftover workers near the ``mineru`` executable; install ``psutil`` for the
sweep. Wall time includes teardown.
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import shutil
import sys
import tempfile
import time
from shlex import join as shlex_join
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from benchmark_utils import (
    apply_mineru_smoke_overrides,
    cleanup_mineru_env_vllm_orphans,
    collect_pdfs_for_benchmark,
    log,
    mineru_subprocess_env,
    namespace_to_config,
    nvidia_physical_gpu_count,
    prepare_shard_dirs,
    repo_test_dir,
    resolve_mineru_cli,
    resolve_mineru_vlm_model_dir,
    run_cmd_with_pgid_cleanup,
    save_benchmark_report,
    shard_evenly,
    wall_seconds,
    print_summary,
    write_mineru_local_vlm_config,
)


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be >= 1")
    return parsed


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Benchmark native MinerU CLI throughput.")
    default_pdf = repo_test_dir() / "sample_pdfs"
    p.add_argument(
        "--pdf-dir",
        type=Path,
        default=default_pdf,
        help=f"Directory containing PDFs (default: repo test/sample_pdfs: {default_pdf})",
    )
    p.add_argument(
        "--output-dir",
        type=Path,
        default=repo_test_dir() / "benchmark_outputs" / "mineru",
        help="Base output directory",
    )
    p.add_argument(
        "--mode",
        choices=("single", "parallel"),
        default="parallel",
        help="single: one mineru on the whole folder; parallel: N shards on N GPUs",
    )
    p.add_argument(
        "--num-shards",
        type=positive_int,
        default=8,
        help="Shard count for parallel mode (should match available GPUs)",
    )
    p.add_argument(
        "--backend",
        default="vlm-auto-engine",
        help="MinerU -b backend (vlm-auto-engine aligns with Flash-MinerU VLM path)",
    )
    p.add_argument(
        "--method",
        default=None,
        help="Optional -m method (auto|txt|ocr) for pipeline/hybrid backends",
    )
    p.add_argument(
        "--lang",
        default=None,
        help="Optional -l lang for pipeline/hybrid backends",
    )
    p.add_argument(
        "--keep-workdir",
        action="store_true",
        help="Keep shard symlink dirs under output-dir (parallel mode)",
    )
    p.add_argument(
        "--keep-nvidia-visible",
        action="store_true",
        help=(
            "Do not unset NVIDIA_VISIBLE_DEVICES for mineru children (default: unset to avoid "
            "CUDA mapping issues when the parent shell injects GPU UUID lists). "
            "Override permanently with env MINERU_BENCHMARK_KEEP_NVIDIA_ENV=1."
        ),
    )
    p.add_argument(
        "--skip-vllm-cleanup",
        action="store_true",
        help=(
            "Do not tear down leftover vLLM/mineru-api workers after the benchmark "
            "(default: each mineru runs in its own process group + optional psutil orphan sweep)."
        ),
    )
    p.add_argument(
        "--vlm-model-dir",
        type=Path,
        default=None,
        help=(
            "Local VLM model directory (e.g. MinerU2.5-2509-1.2B). "
            "Sets MINERU_MODEL_SOURCE=local via generated mineru.local.json. "
            "If omitted, uses MINERU_VLM_MODEL_DIR or a default path when that directory exists."
        ),
    )
    p.add_argument(
        "--report",
        type=Path,
        default=None,
        help="Write JSON report here; default: <output-dir>/reports/mineru_<mode>_<timestamp>.json",
    )
    p.add_argument(
        "--smoke",
        action="store_true",
        help=(
            "Single-PDF smoke: --mode single, one input file (first PDF in --pdf-dir, or "
            "auto-created smoke_minimal.pdf if the directory is empty)."
        ),
    )
    p.add_argument("mineru_extra", nargs=argparse.REMAINDER, help="Extra args after -- for mineru")
    return p.parse_args()


def build_mineru_cmd(
    input_dir: Path,
    output_dir: Path,
    backend: str,
    method: str | None,
    lang: str | None,
    extra: list[str],
) -> list[str]:
    cmd = [resolve_mineru_cli(), "-p", str(input_dir), "-o", str(output_dir), "-b", backend]
    if method:
        cmd.extend(["-m", method])
    if lang:
        cmd.extend(["-l", lang])
    cmd.extend(extra)
    return cmd


def run_single(
    pdf_dir: Path,
    out: Path,
    backend: str,
    method: str | None,
    lang: str | None,
    extra: list[str],
    *,
    keep_nvidia_visible: bool,
    mineru_local_json: Path | None,
) -> int:
    out.mkdir(parents=True, exist_ok=True)
    cmd = build_mineru_cmd(pdf_dir, out, backend, method, lang, extra)
    log(f"Running: {' '.join(cmd)}")
    env = mineru_subprocess_env(
        cuda_visible=None,
        keep_nvidia_visible=keep_nvidia_visible,
        mineru_local_json=mineru_local_json,
    )
    return run_cmd_with_pgid_cleanup(cmd, env)


def _run_shard(
    gpu_id: int,
    shard_in: Path,
    shard_out: Path,
    backend: str,
    method: str | None,
    lang: str | None,
    extra: list[str],
    *,
    keep_nvidia_visible: bool,
    mineru_local_json: Path | None,
) -> tuple[int, int]:
    shard_out.mkdir(parents=True, exist_ok=True)
    env = mineru_subprocess_env(
        cuda_visible=str(gpu_id),
        keep_nvidia_visible=keep_nvidia_visible,
        mineru_local_json=mineru_local_json,
    )
    cmd = build_mineru_cmd(shard_in, shard_out, backend, method, lang, extra)
    log(f"shard gpu={gpu_id} cwd_in={shard_in}: {' '.join(cmd)}")
    rc = run_cmd_with_pgid_cleanup(cmd, env)
    return gpu_id, rc


def run_parallel(
    pdfs: list[Path],
    base_out: Path,
    num_shards: int,
    backend: str,
    method: str | None,
    lang: str | None,
    extra: list[str],
    keep_workdir: bool,
    *,
    keep_nvidia_visible: bool,
    mineru_local_json: Path | None,
) -> int:
    shards = shard_evenly(pdfs, num_shards)
    work_root = base_out / "_shard_inputs"
    base_out.mkdir(parents=True, exist_ok=True)

    shard_entries = prepare_shard_dirs(shards, work_root)
    jobs = [(idx, sin, base_out / f"shard_{idx}") for idx, sin in shard_entries]
    log(
        f"Parallel layout: {len(jobs)} non-empty shard(s) out of {num_shards} bucket(s) "
        f"({len(pdfs)} PDFs)."
    )

    if not jobs:
        log("No non-empty shards; nothing to run.")
        return 1

    max_gpu_index = max(gpu_id for gpu_id, _, _ in jobs)
    ngpu = nvidia_physical_gpu_count()
    if ngpu is not None and max_gpu_index >= ngpu:
        log(
            f"ERROR: parallel mode maps shards to CUDA device indices 0..{max_gpu_index}, "
            f"but nvidia-smi reports only {ngpu} GPU(s). Lower --num-shards or use fewer PDFs "
            f"so the last non-empty shard index is < {ngpu}, or fix visibility (e.g. "
            f"CUDA_VISIBLE_DEVICES on the parent shell)."
        )
        return 2
    if ngpu is not None:
        log(
            f"Launching {len(jobs)} mineru worker(s); max shard GPU index={max_gpu_index} "
            f"(machine has {ngpu} GPU(s) per nvidia-smi)."
        )

    max_workers = len(jobs)
    codes: list[int] = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = [
            ex.submit(
                _run_shard,
                gpu_id,
                sin,
                sout,
                backend,
                method,
                lang,
                extra,
                keep_nvidia_visible=keep_nvidia_visible,
                mineru_local_json=mineru_local_json,
            )
            for gpu_id, sin, sout in jobs
        ]
        for fut in as_completed(futs):
            gpu_id, rc = fut.result()
            log(f"shard gpu={gpu_id} finished with exit code {rc}")
            codes.append(rc)

    if not keep_workdir:
        shutil.rmtree(work_root, ignore_errors=True)

    return 0 if all(c == 0 for c in codes) else max(codes) if codes else 1


def main() -> int:
    args = parse_args()
    if args.mineru_extra and args.mineru_extra[0] == "--":
        args.mineru_extra = args.mineru_extra[1:]

    apply_mineru_smoke_overrides(args)

    corpus_dir = args.pdf_dir.resolve()
    pdfs = collect_pdfs_for_benchmark(corpus_dir, smoke=args.smoke)
    log(f"Found {len(pdfs)} PDF(s) under {corpus_dir} (smoke={args.smoke})")

    smoke_tmp: Path | None = None
    run_pdf_dir = corpus_dir
    if args.smoke:
        smoke_tmp = Path(tempfile.mkdtemp(prefix="mineru_benchmark_smoke_"))
        p0 = pdfs[0]
        link = smoke_tmp / p0.name
        os.symlink(p0.resolve(), link)
        run_pdf_dir = smoke_tmp
        log(f"Smoke input dir (single PDF): {run_pdf_dir}")

    vlm_root = resolve_mineru_vlm_model_dir(args.vlm_model_dir)
    mineru_local_json: Path | None = None
    if vlm_root is not None:
        if not vlm_root.is_dir():
            log(f"ERROR: VLM model path is not a directory: {vlm_root}")
            return 2
        args.output_dir.mkdir(parents=True, exist_ok=True)
        mineru_local_json = (args.output_dir / "mineru.local.json").resolve()
        write_mineru_local_vlm_config(vlm_root, mineru_local_json)
        log(f"Local VLM: {vlm_root}  config -> {mineru_local_json}")
    elif args.mode == "parallel":
        log(
            "WARNING: No local VLM path resolved; each parallel mineru may pull from "
            "HuggingFace — slow, duplicate work, or failures without network."
        )

    t0 = time.perf_counter()
    try:
        if args.mode == "single":
            out = args.output_dir / "single"
            rc = run_single(
                run_pdf_dir,
                out,
                args.backend,
                args.method,
                args.lang,
                args.mineru_extra,
                keep_nvidia_visible=args.keep_nvidia_visible,
                mineru_local_json=mineru_local_json,
            )
        else:
            rc = run_parallel(
                pdfs,
                args.output_dir / "parallel",
                args.num_shards,
                args.backend,
                args.method,
                args.lang,
                args.mineru_extra,
                args.keep_workdir,
                keep_nvidia_visible=args.keep_nvidia_visible,
                mineru_local_json=mineru_local_json,
            )
    finally:
        if smoke_tmp is not None:
            shutil.rmtree(smoke_tmp, ignore_errors=True)

    vllm_cleanup_term = 0
    vllm_cleanup_kill = 0
    if not args.skip_vllm_cleanup:
        vllm_cleanup_term, vllm_cleanup_kill = cleanup_mineru_env_vllm_orphans(
            resolve_mineru_cli()
        )
        if vllm_cleanup_term or vllm_cleanup_kill:
            log(
                "VLM/orphan cleanup (post-run, same conda env as mineru CLI): "
                f"SIGTERM={vllm_cleanup_term}, SIGKILL={vllm_cleanup_kill}"
            )
        elif importlib.util.find_spec("psutil") is None:
            log(
                "Tip: pip install psutil for a second-pass sweep of leftover vLLM workers "
                "(each shard still gets process-group TERM/KILL after mineru exits)."
            )

    elapsed = wall_seconds(t0)
    results = {
        "tool": "mineru",
        "mode": args.mode,
        "num_pdfs": len(pdfs),
        "pdf_dir": str(run_pdf_dir),
        "corpus_pdf_dir": str(corpus_dir),
        "backend": args.backend,
        "wall_seconds": round(elapsed, 3),
        "exit_code": rc,
        "vllm_cleanup_sigterm": vllm_cleanup_term,
        "vllm_cleanup_sigkill": vllm_cleanup_kill,
        "vllm_cleanup_skipped": bool(args.skip_vllm_cleanup),
    }
    if args.mode == "parallel":
        results["num_shards"] = args.num_shards
        results["run_output_dir"] = str((args.output_dir / "parallel").resolve())
    else:
        results["run_output_dir"] = str((args.output_dir / "single").resolve())

    config = namespace_to_config(args)
    config["pdf_dir_resolved"] = str(corpus_dir)
    config["mineru_input_dir_resolved"] = str(run_pdf_dir)
    config["output_dir_resolved"] = str(args.output_dir.resolve())
    config["invocation"] = shlex_join(sys.argv)

    report_path = save_benchmark_report(
        config,
        results,
        report_path=args.report,
        default_dir=args.output_dir / "reports",
        filename_prefix=f"mineru_{args.mode}",
    )
    log(f"Report saved: {report_path}")

    print_summary({"config": config, "results": results})
    return rc


if __name__ == "__main__":
    sys.exit(main())
