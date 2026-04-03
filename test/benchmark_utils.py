"""Shared helpers for MinerU vs Flash-MinerU throughput benchmarks."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path
PDF_SUFFIXES = {".pdf", ".PDF"}


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


def collect_pdfs(pdf_dir: Path) -> list[Path]:
    if not pdf_dir.is_dir():
        raise NotADirectoryError(f"Not a directory: {pdf_dir}")
    paths = sorted(
        p for p in pdf_dir.iterdir() if p.is_file() and p.suffix in PDF_SUFFIXES
    )
    if not paths:
        raise FileNotFoundError(f"No PDF files under {pdf_dir}")
    return paths


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
