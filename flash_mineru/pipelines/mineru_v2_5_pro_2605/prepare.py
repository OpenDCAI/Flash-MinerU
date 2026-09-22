"""Model preparation for MinerU 2.5 Pro 2605."""

from __future__ import annotations

from pathlib import Path


DEFAULT_MODEL = "opendatalab/MinerU2.5-Pro-2605-1.2B"


def prepare_mineru_25_pro_2605(
    *,
    model: str | Path,
    download: bool = False,
    cache_dir: str | Path | None = None,
) -> str:
    """Resolve a local Pro model, downloading only when explicitly requested."""

    requested = str(model)
    local_path = Path(requested).expanduser()
    if local_path.exists():
        if not local_path.is_dir():
            raise ValueError(f"MinerU model must be a directory: {local_path}")
        return str(local_path.resolve())

    if not download:
        raise FileNotFoundError(
            f"MinerU model was not found at {requested!r}. Pass an existing "
            "local directory or call prepare_pipeline(..., download=True)."
        )

    try:
        from huggingface_hub import snapshot_download
    except ImportError as exc:
        raise ImportError(
            "Downloading a MinerU model requires huggingface_hub. "
            "Install it or pass an existing local model directory."
        ) from exc

    resolved = snapshot_download(
        repo_id=requested,
        cache_dir=str(Path(cache_dir).expanduser()) if cache_dir else None,
    )
    return str(Path(resolved).resolve())


__all__ = [
    "DEFAULT_MODEL",
    "prepare_mineru_25_pro_2605",
]
