#!/usr/bin/env python3
"""Reproducible native MinerU vs Flash-MinerU benchmark.

Run ``native`` and ``flash`` separately in the environment appropriate for the
selected MinerU version, then use ``compare`` to calculate the speedup and
output-equivalence report from the two generated JSON reports.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
import traceback
from pathlib import Path

from benchmark_utils import (
    NvidiaSmiSampler,
    aggregate_v4_stage_profiles,
    collect_pdfs,
    compare_outputs,
    corpus_manifest,
    log,
    nvidia_physical_gpu_count,
    prepare_shard_dirs,
    runtime_manifest,
    save_benchmark_report,
    shard_by_page_count,
    validate_v25_outputs,
    validate_v4_outputs,
)

PIPELINES = (
    "v2.5",
    "v2.5-pro-2604",
    "v2.5-pro-2605",
    "v4-flash",
    "v4-basic",
    "v4-standard",
    "v4-advanced",
    "v4-advanced-local",
    "v4-advanced-shared",
)
V4_TIERS = {
    "v4-flash": "flash",
    "v4-basic": "basic",
    "v4-standard": "standard",
    "v4-advanced": "advanced",
    "v4-advanced-local": "advanced",
    "v4-advanced-shared": "advanced",
}


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be >= 1")
    return parsed


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    commands = root.add_subparsers(dest="command", required=True)
    for name in ("native", "flash"):
        command = commands.add_parser(name)
        command.add_argument("--pipeline", choices=PIPELINES, required=True)
        command.add_argument("--pdf-dir", type=Path, required=True)
        command.add_argument("--output-dir", type=Path, required=True)
        command.add_argument("--report", type=Path, required=True)
        command.add_argument("--limit", type=positive_int)
        command.add_argument("--gpus", type=positive_int, default=4)
        command.add_argument("--parse-mode", choices=("auto", "txt", "ocr"), default="auto")
        command.add_argument("--small-backend", choices=("torch", "onnx"), default="torch")
        command.add_argument("--vlm-engine", default="vllm")
        command.add_argument("--vlm-server-url", default="")
        command.add_argument("--vlm-api-key", default="")
        command.add_argument("--vlm-model", default="")
        command.add_argument("--model", required=True)
        command.add_argument("--gpu-sample-interval", type=float, default=1.0)
        command.add_argument(
            "--gpu-memory-utilization",
            type=float,
            default=None,
            help=(
                "Fraction reserved by each local vLLM instance. Defaults to "
                "0.1 for local/shared Advanced and 0.9 for other Flash pipelines."
            ),
        )
        command.add_argument(
            "--image-analysis",
            action=argparse.BooleanOptionalAction,
            default=None,
            help=(
                "Enable image/chart descriptions. By default this is enabled "
                "only for Pro, Standard, and Advanced pipelines."
            ),
        )
        if name == "native":
            command.add_argument("--native-worker", action="store_true", help=argparse.SUPPRESS)
            command.add_argument("--gpu-id", type=int, default=0, help=argparse.SUPPRESS)
            command.add_argument(
                "--worker-batch-size",
                type=positive_int,
                default=16,
                help="PDFs submitted together to each native MinerU 2.5 worker",
            )
        else:
            command.add_argument("--batch-size", type=positive_int, default=16)
            command.add_argument("--input-batch-size", type=positive_int, default=24)
            command.add_argument("--inflight", type=positive_int, default=4)
            command.add_argument(
                "--ocr-batch-size",
                type=positive_int,
                help=(
                    "Analyze-actor batch size. Defaults to 128 for MinerU 2.5, "
                    "32 for V4 Basic, 16 for V4 Advanced Shared, and 8 "
                    "for the other V4 tiers."
                ),
            )
            command.add_argument(
                "--window-size",
                type=positive_int,
                default=8,
                help=(
                    "MinerU 4 pages per lineage child. Eight keeps useful "
                    "fine-grained scheduling without disabling small-model batching."
                ),
            )
            command.add_argument(
                "--layout-batch-size",
                type=positive_int,
                default=8,
                help=(
                    "Pages per local Layout model microbatch after ready "
                    "windows are combined."
                ),
            )

    compare = commands.add_parser("compare")
    compare.add_argument("--native-report", type=Path, required=True)
    compare.add_argument("--flash-report", type=Path, required=True)
    compare.add_argument("--report", type=Path, required=True)
    return root


def selected_pdfs(args: argparse.Namespace) -> list[Path]:
    pdfs = collect_pdfs(args.pdf_dir.expanduser().resolve())
    if args.limit is not None:
        pdfs = pdfs[: args.limit]
    if not pdfs:
        raise ValueError("benchmark corpus is empty")
    return pdfs


def ensure_gpu_count(requested: int) -> None:
    available = nvidia_physical_gpu_count()
    if available is not None and requested > available:
        raise RuntimeError(
            f"requested {requested} GPUs but nvidia-smi reports {available}"
        )


def resolve_image_analysis(args: argparse.Namespace) -> bool:
    """Resolve one explicit image-analysis setting shared by both runners."""

    if args.image_analysis is not None:
        return args.image_analysis
    return args.pipeline in {
        "v2.5-pro-2604",
        "v2.5-pro-2605",
        "v4-standard",
        "v4-advanced",
        "v4-advanced-local",
        "v4-advanced-shared",
    }


def resolve_ocr_batch_size(args: argparse.Namespace) -> int:
    """Choose a safe default at the scheduling boundary of each pipeline."""

    if args.ocr_batch_size is not None:
        return args.ocr_batch_size
    if args.pipeline == "v4-advanced-shared":
        return 16
    if args.pipeline == "v4-basic":
        return 32
    if args.pipeline.startswith("v4-"):
        return 8
    return 128


def resolve_gpu_memory_utilization(args: argparse.Namespace) -> float:
    """Use a small local-vLLM reservation beside MinerU's local model stack."""

    if args.gpu_memory_utilization is not None:
        value = args.gpu_memory_utilization
    elif args.pipeline in {"v4-advanced-local", "v4-advanced-shared"}:
        value = 0.1
    else:
        value = 0.9
    if not 0 < value < 1:
        raise ValueError("gpu_memory_utilization must be between 0 and 1")
    return value


def native_worker(args: argparse.Namespace) -> int:
    pdfs = selected_pdfs(args)
    if args.pipeline.startswith("v4-"):
        from mineru.config import config
        from mineru.parser.mineru_parser import MinerUParser
        from mineru.parser.writer import FileBasedDataWriter

        config.model.base_dir = str(Path(args.model).expanduser().resolve())
        config.model.small_backend = args.small_backend
        config.model.vlm.engine = args.vlm_engine
        config.model.vlm.server_url = args.vlm_server_url
        config.model.vlm.api_key = args.vlm_api_key
        config.model.vlm.model = args.vlm_model
        if args.pipeline in {"v4-advanced-local", "v4-advanced-shared"}:
            if args.vlm_server_url:
                raise ValueError(
                    f"{args.pipeline} requires an actor-local VLM; "
                    "vlm_server_url must be empty"
                )
            if args.vlm_engine != "vllm":
                raise ValueError(
                    f"{args.pipeline} currently requires vlm_engine='vllm'"
                )
            from flash_mineru.pipelines._shared.mineru_v4 import (
                enable_local_vllm_compatibility,
            )

            enable_local_vllm_compatibility()
            # MinerU 4.0 does not expose this local-vLLM knob through
            # VlmConfig. Pin the same explicit reservation used by the
            # RayOrch actor before the runtime module constructs its engine.
            import mineru.model.vlm.runtime as vlm_runtime

            utilization = resolve_gpu_memory_utilization(args)
            vlm_runtime.set_default_gpu_memory_utilization = (
                lambda backend="vllm": utilization
            )
        tier = V4_TIERS[args.pipeline]
        for pdf in pdfs:
            output = args.output_dir / pdf.stem / tier
            output.mkdir(parents=True, exist_ok=True)
            result = MinerUParser(
                tier=tier,
                parse_mode=args.parse_mode,
                image_analysis=args.image_analysis,
            ).parse(pdf)
            result.save(FileBasedDataWriter(str(output)))
        return 0

    # MinerU 2.5/3.x native baseline: use the public VLM entry point once for
    # the worker shard so model initialization is amortized exactly as it is
    # in a normal multi-GPU deployment.
    from mineru.cli.common import do_parse
    from mineru.utils.enum_class import MakeMode

    for start in range(0, len(pdfs), args.worker_batch_size):
        batch = pdfs[start : start + args.worker_batch_size]
        do_parse(
            output_dir=str(args.output_dir),
            pdf_file_names=[pdf.stem for pdf in batch],
            pdf_bytes_list=[pdf.read_bytes() for pdf in batch],
            p_lang_list=["ch"] * len(batch),
            backend="vlm-engine",
            parse_method=args.parse_mode,
            f_draw_layout_bbox=False,
            f_draw_span_bbox=False,
            f_dump_orig_pdf=False,
            f_dump_md=True,
            f_dump_content_list=True,
            f_dump_middle_json=True,
            f_dump_model_output=False,
            f_make_md_mode=MakeMode.MM_MD,
            image_analysis=args.image_analysis,
            model_path=str(Path(args.model).expanduser().resolve()),
            gpu_memory_utilization=float(
                os.getenv("BENCHMARK_GPU_MEMORY_UTILIZATION", "0.9")
            ),
        )
    return 0


def run_native(args: argparse.Namespace) -> int:
    if args.native_worker:
        return native_worker(args)
    ensure_gpu_count(args.gpus)
    pdfs = selected_pdfs(args)
    manifest = corpus_manifest(pdfs)
    # Native MinerU uses one independent process per GPU. Balance by pages so
    # the baseline is not penalized by long PDFs clustering in one file shard.
    shards = shard_by_page_count(pdfs, args.gpus)
    pages_by_path = {
        pdf.resolve(): document["pages"]
        for pdf, document in zip(pdfs, manifest["documents"], strict=True)
    }
    shard_assignments = [
        {
            "gpu_id": gpu_id,
            "documents": len(shard),
            "pages": sum(pages_by_path[pdf.resolve()] for pdf in shard),
        }
        for gpu_id, shard in enumerate(shards)
        if shard
    ]
    work_root = args.output_dir / "_inputs"
    entries = prepare_shard_dirs(shards, work_root, prefix="gpu")
    commands = []
    return_codes: dict[int, int] = {}
    wall_s = 0.0
    sampler = NvidiaSmiSampler(args.gpu_sample_interval)
    started_at = time.perf_counter()
    try:
        with sampler:
            for gpu_id, shard_dir in entries:
                cmd = [
                    sys.executable,
                    str(Path(__file__).resolve()),
                    "native",
                    "--native-worker",
                    "--pipeline",
                    args.pipeline,
                    "--pdf-dir",
                    str(shard_dir),
                    "--output-dir",
                    str(args.output_dir / f"shard_{gpu_id}"),
                    "--report",
                    str(args.report),
                    "--gpus",
                    "1",
                    "--gpu-id",
                    str(gpu_id),
                    "--worker-batch-size",
                    str(args.worker_batch_size),
                    "--parse-mode",
                    args.parse_mode,
                    "--small-backend",
                    args.small_backend,
                    "--vlm-engine",
                    args.vlm_engine,
                    "--vlm-server-url",
                    args.vlm_server_url,
                    "--vlm-api-key",
                    args.vlm_api_key,
                    "--vlm-model",
                    args.vlm_model,
                    "--model",
                    args.model,
                    "--gpu-memory-utilization",
                    str(resolve_gpu_memory_utilization(args)),
                    (
                        "--image-analysis"
                        if args.image_analysis
                        else "--no-image-analysis"
                    ),
                ]
                env = dict(os.environ)
                env["CUDA_VISIBLE_DEVICES"] = str(gpu_id)
                env.pop("NVIDIA_VISIBLE_DEVICES", None)
                commands.append((gpu_id, subprocess.Popen(cmd, env=env)))
            return_codes = {
                gpu_id: process.wait() for gpu_id, process in commands
            }
        wall_s = time.perf_counter() - started_at
    finally:
        shutil.rmtree(work_root, ignore_errors=True)

    validation = validate_outputs(args, pdfs)
    workers_ok = all(code == 0 for code in return_codes.values())
    outputs_ok = (
        validation["successful_documents"] == validation["documents"]
    )
    results = {
        "tool": "native_mineru",
        "pipeline": args.pipeline,
        "environment": runtime_manifest(Path(__file__).resolve().parents[1]),
        "wall_seconds": wall_s,
        "pdf_per_second": len(pdfs) / wall_s,
        "page_per_second": manifest["pages"] / wall_s,
        "worker_exit_codes": return_codes,
        "shard_assignments": shard_assignments,
        "validation": validation,
        "gpu": sampler.summary(),
        "output_dir": str(args.output_dir.resolve()),
    }
    save_benchmark_report(
        vars_json(args),
        results | {"corpus": manifest},
        report_path=args.report,
        default_dir=args.report.parent,
        filename_prefix="native",
    )
    return 0 if workers_ok and outputs_ok else 1


def run_flash(args: argparse.Namespace) -> int:
    ensure_gpu_count(args.gpus)
    pdfs = selected_pdfs(args)
    manifest = corpus_manifest(pdfs)
    from flash_mineru import MineruEngine

    args.output_dir.mkdir(parents=True, exist_ok=True)
    total_started_at = time.perf_counter()
    startup_s = 0.0
    run_s = 0.0
    outputs = []
    run_error: BaseException | None = None
    engine = None
    with NvidiaSmiSampler(args.gpu_sample_interval) as sampler:
        try:
            engine = MineruEngine(
                model=args.model,
                save_dir=str(args.output_dir),
                batch_size=args.batch_size,
                replicas=args.gpus,
                num_gpus_per_replica=1.0,
                engine_gpu_util_rate_to_ray_cap=resolve_gpu_memory_utilization(
                    args
                ),
                inflight=args.inflight,
                pipeline_version=args.pipeline,
                ocr_batch_size=args.ocr_batch_size,
                input_batch_size=args.input_batch_size,
                parse_mode=args.parse_mode,
                window_size=args.window_size,
                layout_batch_size=args.layout_batch_size,
                small_backend=args.small_backend,
                vlm_engine=args.vlm_engine,
                vlm_server_url=args.vlm_server_url,
                vlm_api_key=args.vlm_api_key,
                vlm_model=args.vlm_model,
                image_analysis=args.image_analysis,
            )
            startup_s = time.perf_counter() - total_started_at
            run_started_at = time.perf_counter()
            try:
                outputs = engine.run([str(pdf) for pdf in pdfs])
            finally:
                run_s = time.perf_counter() - run_started_at
        except BaseException as exc:
            run_error = exc
        finally:
            if engine is not None:
                engine.close()
    total_s = time.perf_counter() - total_started_at
    validation = validate_outputs(args, pdfs)
    outputs_ok = (
        validation["successful_documents"] == validation["documents"]
    )
    results = {
        "tool": "flash_mineru",
        "pipeline": args.pipeline,
        "environment": runtime_manifest(Path(__file__).resolve().parents[1]),
        # Executor construction starts Ray actors and waits for their UDF/model
        # initialization. Keep it separate from steady execution while also
        # reporting end-to-end wall time for a fair cold-start comparison.
        "startup_seconds": startup_s,
        "execution_seconds": run_s,
        "wall_seconds": total_s,
        "pdf_per_second": len(pdfs) / run_s if run_s > 0 else None,
        "page_per_second": manifest["pages"] / run_s if run_s > 0 else None,
        "result_batches": len(outputs),
        "validation": validation,
        "gpu": sampler.summary(),
        "output_dir": str(args.output_dir.resolve()),
        "error": (
            {
                "type": type(run_error).__name__,
                "message": str(run_error),
                "traceback": "".join(
                    traceback.format_exception(
                        type(run_error), run_error, run_error.__traceback__
                    )
                ),
            }
            if run_error is not None
            else None
        ),
    }
    if args.pipeline in {
        "v4-basic",
        "v4-standard",
        "v4-advanced",
        "v4-advanced-local",
        "v4-advanced-shared",
    }:
        results["stage_profile"] = aggregate_v4_stage_profiles(
            args.output_dir,
            pdfs,
            V4_TIERS[args.pipeline],
        )
    save_benchmark_report(
        vars_json(args),
        results | {"corpus": manifest},
        report_path=args.report,
        default_dir=args.report.parent,
        filename_prefix="flash",
    )
    if run_error is not None:
        raise run_error
    return 0 if outputs_ok else 1


def validate_outputs(args: argparse.Namespace, pdfs: list[Path]) -> dict:
    if args.pipeline.startswith("v4-"):
        return validate_v4_outputs(
            args.output_dir, pdfs, V4_TIERS[args.pipeline]
        )
    return validate_v25_outputs(args.output_dir, pdfs)


def validate_report_for_comparison(
    label: str,
    results: dict,
    *,
    require_execution_time: bool = False,
) -> None:
    """Reject incomplete benchmark reports before calculating a speedup."""

    error = results.get("error")
    if error:
        if isinstance(error, dict):
            detail = ": ".join(
                value
                for value in (error.get("type"), error.get("message"))
                if value
            )
        else:
            detail = str(error)
        raise ValueError(
            f"{label} benchmark report contains an error"
            + (f": {detail}" if detail else "")
        )

    validation = results.get("validation")
    if not isinstance(validation, dict):
        raise ValueError(f"{label} benchmark report has no output validation")
    documents = validation.get("documents")
    successful = validation.get("successful_documents")
    if (
        not isinstance(documents, int)
        or documents <= 0
        or successful != documents
    ):
        raise ValueError(
            f"{label} benchmark report did not validate every output "
            f"({successful}/{documents})"
        )

    wall_seconds = results.get("wall_seconds")
    if not isinstance(wall_seconds, (int, float)) or wall_seconds <= 0:
        raise ValueError(
            f"{label} benchmark report has invalid wall_seconds: "
            f"{wall_seconds!r}"
        )

    if require_execution_time:
        execution_seconds = results.get("execution_seconds")
        if (
            not isinstance(execution_seconds, (int, float))
            or execution_seconds <= 0
        ):
            raise ValueError(
                f"{label} benchmark report has invalid execution_seconds: "
                f"{execution_seconds!r}"
            )


def run_compare(args: argparse.Namespace) -> int:
    native = json.loads(args.native_report.read_text(encoding="utf-8"))
    flash = json.loads(args.flash_report.read_text(encoding="utf-8"))
    native_results = native["results"]
    flash_results = flash["results"]
    validate_report_for_comparison("native", native_results)
    validate_report_for_comparison(
        "Flash", flash_results, require_execution_time=True
    )
    if native_results["pipeline"] != flash_results["pipeline"]:
        raise ValueError("pipeline mismatch between reports")
    if native_results["corpus"]["sha256"] != flash_results["corpus"]["sha256"]:
        raise ValueError("corpus mismatch between reports")
    pipeline = native_results["pipeline"]
    pdf_dir = Path(native["config"]["pdf_dir"]).expanduser().resolve()
    by_name = {path.name: path for path in collect_pdfs(pdf_dir)}
    pdfs = []
    for document in native_results["corpus"]["documents"]:
        try:
            pdfs.append(by_name[document["name"]])
        except KeyError as exc:
            raise FileNotFoundError(
                f"corpus PDF is no longer present: {document['name']}"
            ) from exc
    native_wall = native_results["wall_seconds"]
    flash_wall = flash_results["wall_seconds"]
    native_config = native["config"]
    flash_config = flash["config"]
    comparable_keys = (
        "pipeline",
        "parse_mode",
        "small_backend",
        "vlm_engine",
        "vlm_server_url",
        "vlm_model",
        "model",
        "gpus",
        "image_analysis",
    )
    mismatches = {
        key: {
            "native": native_config.get(key),
            "flash": flash_config.get(key),
        }
        for key in comparable_keys
        if native_config.get(key) != flash_config.get(key)
    }
    if mismatches:
        raise ValueError(
            "native and Flash benchmark configurations are not comparable: "
            + json.dumps(mismatches, ensure_ascii=False, sort_keys=True)
        )
    comparison = compare_outputs(
        Path(flash_results["output_dir"]),
        Path(native_results["output_dir"]),
        pdfs,
        pipeline_version=pipeline,
        v4_tier=V4_TIERS.get(pipeline),
    )
    payload = {
        "pipeline": pipeline,
        "corpus": native_results["corpus"],
        "native_wall_seconds": native_wall,
        "flash_execution_seconds": flash_results["execution_seconds"],
        "flash_end_to_end_seconds": flash_wall,
        "speedup_native_over_flash_end_to_end": native_wall / flash_wall,
        "speedup_native_over_flash_execution": (
            native_wall / flash_results["execution_seconds"]
        ),
        "output_comparison": comparison,
        "native_report": str(args.native_report.resolve()),
        "flash_report": str(args.flash_report.resolve()),
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if comparison["valid_documents"] == comparison["documents"] else 1


def vars_json(args: argparse.Namespace) -> dict:
    return {
        key: str(value) if isinstance(value, Path) else value
        for key, value in vars(args).items()
        if not key.startswith("_")
    }


def main() -> int:
    args = parser().parse_args()
    if args.command in {"native", "flash"}:
        args.image_analysis = resolve_image_analysis(args)
    if args.command == "flash":
        args.ocr_batch_size = resolve_ocr_batch_size(args)
    if args.command == "native":
        return run_native(args)
    if args.command == "flash":
        return run_flash(args)
    return run_compare(args)


if __name__ == "__main__":
    sys.exit(main())
