"""Shared helpers for MinerU vs Flash-MinerU throughput benchmarks."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import os
import platform
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
from collections import Counter
from pathlib import Path
PDF_SUFFIXES = {".pdf", ".PDF"}
BENCHMARK_OUTPUT_FILES = (
    "markdown.md",
    "middle_json.json",
    "model_output.json",
    "structured_content.json",
)
_TOKEN = re.compile(r"\\[A-Za-z]+|[A-Za-z0-9_]+|[^\s]", re.UNICODE)


def write_mineru_local_vlm_config(vlm_dir: Path, config_path: Path) -> None:
    """
    Write a minimal mineru.json for MINERU_MODEL_SOURCE=local (MinerU reads models-dir.vlm).
    config_path is typically MINERU_TOOLS_CONFIG_JSON (absolute path recommended).
    """
    root = vlm_dir.expanduser().resolve()
    if not root.is_dir():
        raise NotADirectoryError(f"VLM model directory does not exist: {root}")
    payload = {"models-dir": {"vlm": str(root)}}
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def resolve_mineru_vlm_model_dir(cli_path: Path | None) -> Path | None:
    """
    Effective VLM root: --vlm-model-dir, else MINERU_VLM_MODEL_DIR, else default path if present.
    Returns None to use MinerU default (remote Hub download).
    """
    if cli_path is not None:
        return cli_path.expanduser().resolve()
    env_path = os.environ.get("MINERU_VLM_MODEL_DIR", "").strip()
    if env_path:
        return Path(env_path).expanduser().resolve()
    fallback = Path("/home/dataset-assist-0/usr/models/MinerU2.5-2509-1.2B")
    if fallback.is_dir():
        return fallback.resolve()
    return None


def mineru_subprocess_env(
    *,
    cuda_visible: str | None = None,
    keep_nvidia_visible: bool = False,
    mineru_local_json: Path | None = None,
) -> dict[str, str]:
    """
    Environment for spawning `mineru` / local VLM workers.

    Some Batch/IDE hosts set NVIDIA_VISIBLE_DEVICES to a UUID list while benchmarks also set
    CUDA_VISIBLE_DEVICES per shard; that combination can break CUDA device mapping so that
    inference stays CPU-bound and nvidia-smi shows no compute processes. Clearing
    NVIDIA_VISIBLE_DEVICES for the child restores plain index-based CUDA_VISIBLE_DEVICES.
    Set keep_nvidia_visible=True (or env MINERU_BENCHMARK_KEEP_NVIDIA_ENV=1) to preserve it.
    """
    env = dict(os.environ)
    env.setdefault("PYTHONUNBUFFERED", "1")
    if not keep_nvidia_visible:
        if os.getenv("MINERU_BENCHMARK_KEEP_NVIDIA_ENV", "").lower() not in (
            "1",
            "true",
            "yes",
        ):
            env.pop("NVIDIA_VISIBLE_DEVICES", None)
    if cuda_visible is not None:
        env["CUDA_VISIBLE_DEVICES"] = cuda_visible
    if mineru_local_json is not None:
        cfg = Path(mineru_local_json).expanduser().resolve()
        if not cfg.is_file():
            raise FileNotFoundError(f"MinerU local config not found: {cfg}")
        env["MINERU_MODEL_SOURCE"] = "local"
        env["MINERU_TOOLS_CONFIG_JSON"] = str(cfg)
    return env


def resolve_mineru_cli() -> str:
    """
    Path to the `mineru` executable. Prefer the script next to sys.executable (conda env bin)
    so subprocess works when PATH is minimal (e.g. nohup without conda shell hook).
    """
    bindir = Path(sys.executable).resolve().parent
    candidate = bindir / "mineru"
    if candidate.is_file():
        return str(candidate)
    w = shutil.which("mineru")
    if w:
        return w
    raise FileNotFoundError(
        "mineru CLI not found: expected next to this Python or on PATH (activate mineru conda env)."
    )


def repo_test_dir() -> Path:
    return Path(__file__).resolve().parent


def nvidia_physical_gpu_count() -> int | None:
    """
    Count GPUs via nvidia-smi (ignores CUDA_VISIBLE_DEVICES in this process).
    Returns None if nvidia-smi is missing or fails.
    """
    try:
        proc = subprocess.run(
            ["nvidia-smi", "-L"],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return None
    if proc.returncode != 0 or not proc.stdout:
        return None
    n = sum(1 for line in proc.stdout.splitlines() if line.startswith("GPU "))
    return n if n > 0 else None


def runtime_manifest(repo_root: Path | None = None) -> dict:
    """Capture enough software and hardware identity to reproduce a run."""

    packages = {}
    for name in (
        "flash-mineru",
        "mineru",
        "mineru-vl-utils",
        "ray",
        "rayorch",
        "torch",
        "vllm",
    ):
        try:
            packages[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            packages[name] = None

    gpu = {
        "driver_version": None,
        "cuda_version": None,
        "devices": [],
    }
    try:
        completed = subprocess.run(
            [
                "nvidia-smi",
                "--query-gpu=index,name,uuid,driver_version",
                "--format=csv,noheader,nounits",
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
        if completed.returncode == 0:
            for line in completed.stdout.splitlines():
                fields = [field.strip() for field in line.split(",")]
                if len(fields) == 4:
                    gpu["devices"].append(
                        {
                            "index": int(fields[0]),
                            "name": fields[1],
                            "uuid": fields[2],
                            "driver_version": fields[3],
                        }
                    )
            if gpu["devices"]:
                gpu["driver_version"] = gpu["devices"][0]["driver_version"]
        version_query = subprocess.run(
            ["nvidia-smi"],
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
        match = re.search(r"CUDA Version:\s*([0-9.]+)", version_query.stdout)
        if match:
            gpu["cuda_version"] = match.group(1)
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        pass

    commit = None
    dirty = None
    if repo_root is not None:
        try:
            revision = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=repo_root,
                capture_output=True,
                text=True,
                check=False,
                timeout=30,
            )
            if revision.returncode == 0:
                commit = revision.stdout.strip()
            status = subprocess.run(
                ["git", "status", "--porcelain"],
                cwd=repo_root,
                capture_output=True,
                text=True,
                check=False,
                timeout=30,
            )
            if status.returncode == 0:
                dirty = bool(status.stdout.strip())
        except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
            pass

    return {
        "python": sys.version,
        "python_executable": sys.executable,
        "platform": platform.platform(),
        "packages": packages,
        "gpu": gpu,
        "repository": {
            "path": str(repo_root.resolve()) if repo_root is not None else None,
            "commit": commit,
            "dirty": dirty,
        },
    }


def collect_pdfs(pdf_dir: Path) -> list[Path]:
    if not pdf_dir.is_dir():
        raise NotADirectoryError(f"Not a directory: {pdf_dir}")
    paths = sorted(
        p for p in pdf_dir.iterdir() if p.is_file() and p.suffix in PDF_SUFFIXES
    )
    if not paths:
        raise FileNotFoundError(f"No PDF files under {pdf_dir}")
    return paths


def pdf_page_count(path: Path) -> int:
    """Count pages without importing MinerU or model runtimes."""

    try:
        import pypdfium2 as pdfium

        document = pdfium.PdfDocument(str(path))
        try:
            return len(document)
        finally:
            document.close()
    except ImportError:
        from pypdf import PdfReader

        return len(PdfReader(str(path)).pages)


def corpus_manifest(pdfs: list[Path]) -> dict:
    """Stable corpus identity used to prove native/Flash runs used the same input."""

    digest = hashlib.sha256()
    total_bytes = 0
    total_pages = 0
    documents = []
    for path in pdfs:
        resolved = path.expanduser().resolve()
        size = resolved.stat().st_size
        pages = pdf_page_count(resolved)
        total_bytes += size
        total_pages += pages
        digest.update(resolved.name.encode("utf-8"))
        digest.update(b"\0")
        with resolved.open("rb") as source:
            while chunk := source.read(1024 * 1024):
                digest.update(chunk)
        documents.append({"name": resolved.name, "bytes": size, "pages": pages})
    return {
        "pdfs": len(pdfs),
        "pages": total_pages,
        "bytes": total_bytes,
        "sha256": digest.hexdigest(),
        "documents": documents,
    }


class NvidiaSmiSampler:
    """Lightweight GPU utilization/memory sampler for benchmark reports."""

    def __init__(self, interval_s: float = 1.0) -> None:
        self.interval_s = interval_s
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._samples: list[dict] = []

    def __enter__(self):
        self._thread = threading.Thread(target=self._sample, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self.interval_s + 2)

    def _sample(self) -> None:
        query = (
            "index,name,uuid,memory.used,memory.total,utilization.gpu,"
            "utilization.memory,power.draw"
        )
        while not self._stop.is_set():
            try:
                completed = subprocess.run(
                    [
                        "nvidia-smi",
                        f"--query-gpu={query}",
                        "--format=csv,noheader,nounits",
                    ],
                    capture_output=True,
                    text=True,
                    check=False,
                    timeout=10,
                )
                if completed.returncode == 0:
                    self._samples.append(
                        {
                            "timestamp": time.time(),
                            "gpus": [
                                _parse_nvidia_smi_row(line)
                                for line in completed.stdout.splitlines()
                                if line.strip()
                            ],
                        }
                    )
            except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
                pass
            self._stop.wait(self.interval_s)

    def summary(self) -> dict:
        gpus: dict[int, dict] = {}
        for sample in self._samples:
            for row in sample["gpus"]:
                gpu = gpus.setdefault(
                    row["index"],
                    {
                        "index": row["index"],
                        "name": row["name"],
                        "uuid": row["uuid"],
                        "memory_total_mib": row["memory_total_mib"],
                        "peak_memory_used_mib": 0.0,
                        "peak_gpu_utilization_percent": 0.0,
                        "mean_gpu_utilization_percent": 0.0,
                        "peak_memory_utilization_percent": 0.0,
                        "peak_power_w": 0.0,
                        "_utilization_sum": 0.0,
                        "_samples": 0,
                    },
                )
                gpu["peak_memory_used_mib"] = max(
                    gpu["peak_memory_used_mib"], row["memory_used_mib"]
                )
                gpu["peak_gpu_utilization_percent"] = max(
                    gpu["peak_gpu_utilization_percent"],
                    row["gpu_utilization_percent"],
                )
                gpu["peak_memory_utilization_percent"] = max(
                    gpu["peak_memory_utilization_percent"],
                    row["memory_utilization_percent"],
                )
                gpu["peak_power_w"] = max(gpu["peak_power_w"], row["power_w"])
                gpu["_utilization_sum"] += row["gpu_utilization_percent"]
                gpu["_samples"] += 1
        for gpu in gpus.values():
            samples = gpu.pop("_samples")
            gpu["mean_gpu_utilization_percent"] = round(
                gpu.pop("_utilization_sum") / samples, 3
            )
        return {
            "sample_interval_s": self.interval_s,
            "sample_count": len(self._samples),
            "gpus": [gpus[index] for index in sorted(gpus)],
        }


def _parse_nvidia_smi_row(line: str) -> dict:
    fields = [field.strip() for field in line.split(",")]
    return {
        "index": int(fields[0]),
        "name": fields[1],
        "uuid": fields[2],
        "memory_used_mib": float(fields[3]),
        "memory_total_mib": float(fields[4]),
        "gpu_utilization_percent": float(fields[5]),
        "memory_utilization_percent": float(fields[6]),
        "power_w": float(fields[7]),
    }


def locate_v25_output(root: Path, stem: str) -> Path | None:
    """Find a MinerU 2.5 style output directory under flat or sharded roots."""

    direct = root / stem / "vlm"
    if direct.is_dir():
        return direct
    matches = sorted(root.glob(f"shard_*/{stem}/vlm"))
    return matches[0] if matches else None


def locate_v25_middle(output: Path, stem: str) -> Path:
    """Resolve the equivalent middle artifact emitted by Flash or native MinerU."""

    flash_layout = output / "layout.json"
    if flash_layout.is_file():
        return flash_layout
    return output / f"{stem}_middle.json"


def locate_v4_output(root: Path, stem: str, tier: str) -> Path | None:
    """Find a MinerU 4 style output directory under flat or sharded roots."""

    candidates = (
        root / stem / tier,
        root / stem,
        root,
    )
    for candidate in candidates:
        if (candidate / "markdown.md").is_file():
            return candidate
    matches = sorted(root.glob(f"shard_*/{stem}/{tier}"))
    if matches:
        return matches[0]
    matches = sorted(root.glob(f"shard_*/{stem}"))
    return next(
        (
            candidate
            for candidate in matches
            if (candidate / "markdown.md").is_file()
        ),
        None,
    )


def validate_v25_outputs(root: Path, pdfs: list[Path]) -> dict:
    """Validate the three stable MinerU 2.5 artifacts for every PDF."""

    rows = []
    total_pages = 0
    for pdf in pdfs:
        output = locate_v25_output(root, pdf.stem)
        errors = []
        pages = None
        if output is None:
            errors.append("output directory missing")
        else:
            markdown = output / f"{pdf.stem}.md"
            content = output / f"{pdf.stem}_content_list.json"
            middle = locate_v25_middle(output, pdf.stem)
            for path in (markdown, content, middle):
                if not path.is_file() or path.stat().st_size == 0:
                    errors.append(f"missing or empty: {path.name}")
            if not errors:
                try:
                    json.loads(content.read_text(encoding="utf-8"))
                    middle_value = json.loads(middle.read_text(encoding="utf-8"))
                    pages = len(middle_value.get("pdf_info", []))
                    total_pages += pages
                except (json.JSONDecodeError, OSError) as exc:
                    errors.append(f"invalid JSON: {exc}")
        rows.append({"stem": pdf.stem, "pages": pages, "errors": errors})
    return _validation_summary(rows, total_pages)


def validate_v4_outputs(root: Path, pdfs: list[Path], tier: str) -> dict:
    """Validate the four official MinerU 4 artifacts for every PDF."""

    rows = []
    total_pages = 0
    for pdf in pdfs:
        output = locate_v4_output(root, pdf.stem, tier)
        errors = []
        pages = None
        if output is None:
            errors.append("output directory missing")
        else:
            for name in BENCHMARK_OUTPUT_FILES:
                path = output / name
                if not path.is_file() or path.stat().st_size == 0:
                    errors.append(f"missing or empty: {name}")
            if not errors:
                try:
                    middle = json.loads(
                        (output / "middle_json.json").read_text(encoding="utf-8")
                    )
                    json.loads(
                        (output / "model_output.json").read_text(encoding="utf-8")
                    )
                    json.loads(
                        (output / "structured_content.json").read_text(
                            encoding="utf-8"
                        )
                    )
                    pages = len(middle.get("pdf_info", middle.get("pages", [])))
                    total_pages += pages
                except (json.JSONDecodeError, OSError) as exc:
                    errors.append(f"invalid JSON: {exc}")
        rows.append({"stem": pdf.stem, "pages": pages, "errors": errors})
    return _validation_summary(rows, total_pages)


def aggregate_v4_stage_profiles(
    root: Path,
    pdfs: list[Path],
    tier: str,
) -> dict:
    """Aggregate per-document Flash-MinerU stage profiles.

    Stage values are cumulative actor wall time. They describe where work was
    spent, but their sum is not the pipeline critical path because RayOrch can
    overlap CPU Render, GPU Analyze, and document assembly.
    """

    stages: dict[str, float] = {}
    categories: dict[str, float] = {}
    documents = 0
    missing_documents = []
    for pdf in pdfs:
        output = locate_v4_output(root, pdf.stem, tier)
        manifest_path = output / "flash_mineru.json" if output else None
        if manifest_path is None or not manifest_path.is_file():
            missing_documents.append(pdf.stem)
            continue
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            profile = manifest["stage_profile"]
        except (KeyError, OSError, json.JSONDecodeError, TypeError):
            missing_documents.append(pdf.stem)
            continue
        documents += 1
        for name, seconds in profile.get("stages_seconds", {}).items():
            stages[name] = stages.get(name, 0.0) + float(seconds)
        for name, seconds in profile.get("categories_seconds", {}).items():
            categories[name] = categories.get(name, 0.0) + float(seconds)

    return {
        "kind": "cumulative_actor_stage_wall_time",
        "documents": documents,
        "missing_documents": missing_documents,
        "stages_seconds": {
            name: round(seconds, 6) for name, seconds in sorted(stages.items())
        },
        "categories_seconds": {
            name: round(seconds, 6)
            for name, seconds in sorted(categories.items())
        },
    }


def _validation_summary(rows: list[dict], total_pages: int) -> dict:
    return {
        "documents": len(rows),
        "successful_documents": sum(not row["errors"] for row in rows),
        "pages": total_pages,
        "failed_documents": [
            {"stem": row["stem"], "errors": row["errors"]}
            for row in rows
            if row["errors"]
        ],
    }


def compare_outputs(
    candidate_root: Path,
    reference_root: Path,
    pdfs: list[Path],
    *,
    pipeline_version: str,
    v4_tier: str | None = None,
) -> dict:
    """Compare Markdown and structural counts without assuming VLM determinism."""

    if pipeline_version.startswith("v4-") and not v4_tier:
        raise ValueError("v4_tier is required for MinerU 4 output comparison")
    rows = []
    for pdf in pdfs:
        if pipeline_version.startswith("v4-"):
            candidate = locate_v4_output(candidate_root, pdf.stem, v4_tier)
            reference = locate_v4_output(reference_root, pdf.stem, v4_tier)
            markdown_name = "markdown.md"
            middle_name = "middle_json.json"
            model_name = "model_output.json"
        else:
            candidate = locate_v25_output(candidate_root, pdf.stem)
            reference = locate_v25_output(reference_root, pdf.stem)
            markdown_name = f"{pdf.stem}.md"
            model_name = f"{pdf.stem}_content_list.json"
        row = {"stem": pdf.stem, "errors": []}
        if candidate is None or reference is None:
            row["errors"].append("candidate or reference output directory missing")
            rows.append(row)
            continue
        try:
            candidate_markdown = (candidate / markdown_name).read_text(
                encoding="utf-8"
            )
            reference_markdown = (reference / markdown_name).read_text(
                encoding="utf-8"
            )
            if pipeline_version.startswith("v4-"):
                candidate_middle_path = candidate / middle_name
                reference_middle_path = reference / middle_name
            else:
                candidate_middle_path = locate_v25_middle(candidate, pdf.stem)
                reference_middle_path = locate_v25_middle(reference, pdf.stem)
            candidate_middle = json.loads(
                candidate_middle_path.read_text(encoding="utf-8")
            )
            reference_middle = json.loads(
                reference_middle_path.read_text(encoding="utf-8")
            )
            candidate_model = json.loads(
                (candidate / model_name).read_text(encoding="utf-8")
            )
            reference_model = json.loads(
                (reference / model_name).read_text(encoding="utf-8")
            )
        except (OSError, json.JSONDecodeError) as exc:
            row["errors"].append(f"{type(exc).__name__}: {exc}")
            rows.append(row)
            continue
        candidate_tokens = _TOKEN.findall(candidate_markdown)
        reference_tokens = _TOKEN.findall(reference_markdown)
        candidate_pages = _json_page_count(candidate_middle)
        reference_pages = _json_page_count(reference_middle)
        candidate_blocks = _json_block_count(candidate_model)
        reference_blocks = _json_block_count(reference_model)
        row.update(
            exact_markdown=candidate_markdown == reference_markdown,
            token_jaccard=_jaccard(candidate_tokens, reference_tokens),
            token_multiset_f1=_multiset_f1(candidate_tokens, reference_tokens),
            candidate_pages=candidate_pages,
            reference_pages=reference_pages,
            candidate_blocks=candidate_blocks,
            reference_blocks=reference_blocks,
        )
        if candidate_pages != reference_pages:
            row["errors"].append("page-count mismatch")
        rows.append(row)

    comparable = [row for row in rows if "token_jaccard" in row]
    return {
        "documents": len(rows),
        "comparable_documents": len(comparable),
        "valid_documents": sum(not row["errors"] for row in rows),
        "exact_markdown_documents": sum(
            row.get("exact_markdown", False) for row in rows
        ),
        "token_jaccard": _range(
            [row["token_jaccard"] for row in comparable]
        ),
        "token_multiset_f1": _range(
            [row["token_multiset_f1"] for row in comparable]
        ),
        "page_count_mismatches": [
            {
                "stem": row["stem"],
                "candidate": row["candidate_pages"],
                "reference": row["reference_pages"],
            }
            for row in comparable
            if row["candidate_pages"] != row["reference_pages"]
        ],
        "block_count_differences": [
            {
                "stem": row["stem"],
                "candidate": row["candidate_blocks"],
                "reference": row["reference_blocks"],
            }
            for row in comparable
            if row["candidate_blocks"] != row["reference_blocks"]
        ],
        "documents_with_errors": [
            {"stem": row["stem"], "errors": row["errors"]}
            for row in rows
            if row["errors"]
        ],
        "per_document": rows,
    }


def _json_page_count(value) -> int | None:
    if isinstance(value, dict):
        for key in ("pages", "pdf_info"):
            items = value.get(key)
            if isinstance(items, list):
                return len(items)
    if isinstance(value, list):
        return len(value)
    return None


def _json_block_count(value) -> int | None:
    if isinstance(value, list):
        return len(value)
    if not isinstance(value, dict):
        return None
    pages = value.get("pages") or value.get("pdf_info")
    if not isinstance(pages, list):
        return None
    count = 0
    for page in pages:
        if isinstance(page, list):
            count += len(page)
            continue
        if not isinstance(page, dict):
            continue
        blocks = (
            page.get("blocks")
            or page.get("para_blocks")
            or page.get("preproc_blocks")
            or []
        )
        if isinstance(blocks, list):
            count += len(blocks)
    return count


def _jaccard(left: list[str], right: list[str]) -> float:
    left_set, right_set = set(left), set(right)
    union = left_set | right_set
    return len(left_set & right_set) / len(union) if union else 1.0


def _multiset_f1(left: list[str], right: list[str]) -> float:
    left_counter, right_counter = Counter(left), Counter(right)
    common = sum((left_counter & right_counter).values())
    total = sum(left_counter.values()) + sum(right_counter.values())
    return 2 * common / total if total else 1.0


def _range(values: list[float]) -> dict:
    if not values:
        return {
            "min": None,
            "p05": None,
            "median": None,
            "mean": None,
            "max": None,
        }
    ordered = sorted(values)
    return {
        "min": ordered[0],
        "p05": _percentile(ordered, 0.05),
        "median": _percentile(ordered, 0.5),
        "mean": sum(ordered) / len(ordered),
        "max": ordered[-1],
    }


def _percentile(ordered: list[float], quantile: float) -> float:
    position = (len(ordered) - 1) * quantile
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (
        position - lower
    )


def write_minimal_smoke_pdf(dest: Path) -> None:
    """Write a one-page minimal valid PDF (stdlib only) for smoke tests."""
    dest = dest.expanduser().resolve()
    dest.parent.mkdir(parents=True, exist_ok=True)
    pieces: list[bytes] = []
    pieces.append(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
    offsets: list[int] = []
    for num, inner in (
        (1, b"<< /Type /Catalog /Pages 2 0 R >>\n"),
        (2, b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>\n"),
        (3, b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] >>\n"),
    ):
        offsets.append(sum(len(p) for p in pieces))
        pieces.append(f"{num} 0 obj\n".encode("ascii") + inner + b"endobj\n")
    xref_start = sum(len(p) for p in pieces)
    xref_parts = [b"xref\n", b"0 4\n", b"0000000000 65535 f \n"]
    for off in offsets:
        xref_parts.append(f"{off:010d} 00000 n \n".encode("ascii"))
    pieces.append(b"".join(xref_parts))
    pieces.append(
        f"trailer\n<< /Size 4 /Root 1 0 R >>\nstartxref\n{xref_start}\n%%EOF\n".encode(
            "ascii"
        )
    )
    dest.write_bytes(b"".join(pieces))


def collect_pdfs_for_benchmark(pdf_dir: Path, *, smoke: bool = False) -> list[Path]:
    """
    Like ``collect_pdfs``, but for ``smoke=True``:
    - creates ``pdf_dir`` if missing;
    - if there are no PDFs, writes ``smoke_minimal.pdf`` into ``pdf_dir``;
    - returns at most one PDF (first in sort order).
    """
    pdf_dir = pdf_dir.expanduser().resolve()
    if not pdf_dir.is_dir():
        if smoke:
            pdf_dir.mkdir(parents=True, exist_ok=True)
        else:
            raise NotADirectoryError(f"Not a directory: {pdf_dir}")
    paths = sorted(
        p for p in pdf_dir.iterdir() if p.is_file() and p.suffix in PDF_SUFFIXES
    )
    if not paths:
        if not smoke:
            raise FileNotFoundError(f"No PDF files under {pdf_dir}")
        p = pdf_dir / "smoke_minimal.pdf"
        write_minimal_smoke_pdf(p)
        paths = [p]
    if smoke:
        return paths[:1]
    return paths


def shard_evenly(items: list[Path], num_shards: int) -> list[list[Path]]:
    if num_shards < 1:
        raise ValueError("num_shards must be >= 1")
    n = len(items)
    base, extra = divmod(n, num_shards)
    out: list[list[Path]] = []
    idx = 0
    for i in range(num_shards):
        take = base + (1 if i < extra else 0)
        out.append(items[idx : idx + take])
        idx += take
    return out


def shard_by_page_count(
    items: list[Path],
    num_shards: int,
) -> list[list[Path]]:
    """Balance PDF shards by pages rather than only by document count.

    Native MinerU keeps one model process per GPU and processes its assigned
    documents locally. A contiguous file-count split can leave one GPU with
    substantially more pages, so use deterministic longest-processing-time
    assignment to make the multi-GPU baseline as strong and fair as possible.
    """

    if num_shards < 1:
        raise ValueError("num_shards must be >= 1")
    weighted = sorted(
        ((pdf_page_count(path), path) for path in items),
        key=lambda item: (-item[0], item[1].name),
    )
    shards: list[list[Path]] = [[] for _ in range(num_shards)]
    page_totals = [0] * num_shards
    for pages, path in weighted:
        shard_index = min(
            range(num_shards),
            key=lambda index: (
                page_totals[index],
                len(shards[index]),
                index,
            ),
        )
        shards[shard_index].append(path)
        page_totals[shard_index] += pages
    return shards


def prepare_shard_dirs(
    shards: list[list[Path]], work_root: Path, prefix: str = "shard"
) -> list[tuple[int, Path]]:
    """Create one directory per non-empty shard with symlinks; keep shard index for GPU mapping."""
    work_root.mkdir(parents=True, exist_ok=True)
    out: list[tuple[int, Path]] = []
    for i, paths in enumerate(shards):
        if not paths:
            continue
        d = work_root / f"{prefix}_{i}"
        if d.exists():
            shutil.rmtree(d)
        d.mkdir(parents=True)
        for p in paths:
            link = d / p.name
            if link.exists():
                link.unlink()
            os.symlink(p.resolve(), link)
        out.append((i, d))
    return out


def print_summary(payload: dict) -> None:
    line = json.dumps(payload, ensure_ascii=False, indent=2)
    print(line, flush=True)


def wall_seconds(start: float) -> float:
    return time.perf_counter() - start


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def signal_process_group(pgid: int, sig: int) -> None:
    try:
        os.killpg(pgid, sig)
    except (ProcessLookupError, PermissionError, OSError):
        pass


def run_cmd_with_pgid_cleanup(cmd: list[str], env: dict[str, str]) -> int:
    """
    Run a command in a new session so all descendants share a process group.
    After the main process exits, signal the group (TERM then KILL) so leftover
    vLLM / API children are less likely to outlive the parent mineru CLI.
    """
    proc = subprocess.Popen(cmd, env=env, start_new_session=True)
    pgid = proc.pid
    try:
        return proc.wait()
    finally:
        signal_process_group(pgid, signal.SIGTERM)
        time.sleep(1.25)
        signal_process_group(pgid, signal.SIGKILL)


def cleanup_mineru_env_vllm_orphans(mineru_cli: Path | str) -> tuple[int, int]:
    """
    Second-pass cleanup: SIGTERM then SIGKILL processes that still look like
    vLLM / FastAPI workers running from the same conda env as ``mineru_cli``.

    Requires psutil; if missing, returns (0, 0). Safe-ish for single-user
    benchmark hosts (only touches PIDs under that env path).
    """
    env_root = str(Path(mineru_cli).resolve().parent.parent)
    sep = os.sep
    if not env_root.endswith(sep):
        env_prefix = env_root + sep
    else:
        env_prefix = env_root

    try:
        import psutil
    except ImportError:
        return (0, 0)

    def proc_marks_env(proc) -> bool:
        try:
            exe = str(Path(proc.exe()).resolve())
            if exe.startswith(env_prefix):
                return True
        except (psutil.Error, OSError, ValueError):
            pass
        try:
            cmd = proc.cmdline()
            return any(env_root in a for a in cmd)
        except (psutil.Error, OSError):
            return False

    def looks_like_stack(cmd: list[str]) -> bool:
        low = " ".join(cmd).lower()
        keys = (
            "vllm",
            "uvicorn",
            "mineru-api",
            "fastapi",
            "openai",
            "enginecore",
            "multiprocessing.spawn",
        )
        return any(k in low for k in keys)

    victims: list = []
    for proc in psutil.process_iter():
        try:
            if not proc_marks_env(proc):
                continue
            cmd = proc.cmdline()
            if not cmd or not looks_like_stack(cmd):
                continue
            victims.append(proc)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

    n_term = 0
    for proc in victims:
        try:
            proc.send_signal(signal.SIGTERM)
            n_term += 1
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

    if n_term:
        time.sleep(2.0)

    n_kill = 0
    for proc in victims:
        try:
            if proc.is_running():
                proc.send_signal(signal.SIGKILL)
                n_kill += 1
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

    return (n_term, n_kill)


def namespace_to_config(ns: argparse.Namespace) -> dict:
    """JSON-friendly snapshot of argparse flags (Paths → str)."""
    out: dict = {}
    for key in sorted(vars(ns)):
        if key.startswith("_"):
            continue
        val = getattr(ns, key)
        if isinstance(val, Path):
            out[key] = str(val)
        elif isinstance(val, (str, int, float, bool, type(None))):
            out[key] = val
        elif isinstance(val, list):
            out[key] = [str(x) if isinstance(x, Path) else x for x in val]
        else:
            out[key] = str(val)
    return out


def apply_flash_smoke_overrides(ns: argparse.Namespace) -> None:
    """Mutate namespace: one PDF, one batch, one replica (Flash / DAG benches)."""
    if not getattr(ns, "smoke", False):
        return
    ns.profile = False
    ns.max_batches = 1
    ns.replicas = 1
    ns.batch_size = 1
    if hasattr(ns, "inflight"):
        ns.inflight = 1


def apply_mineru_smoke_overrides(ns: argparse.Namespace) -> None:
    """Mutate namespace: single-process run, one PDF (native MinerU bench)."""
    if not getattr(ns, "smoke", False):
        return
    ns.mode = "single"
    ns.num_shards = 1


def save_benchmark_report(
    config: dict,
    results: dict,
    *,
    report_path: Path | None,
    default_dir: Path,
    filename_prefix: str,
) -> Path:
    """
    Write JSON with keys in order: config, then results.
    If report_path is None, uses default_dir / f\"{filename_prefix}_{timestamp}.json\".
    """
    if report_path is not None:
        path = report_path.expanduser().resolve()
    else:
        default_dir.mkdir(parents=True, exist_ok=True)
        ts = time.strftime("%Y%m%d_%H%M%S")
        path = default_dir / f"{filename_prefix}_{ts}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"config": config, "results": results}
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.write("\n")
    return path
