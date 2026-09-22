"""Model preparation for MinerU 4 Standard."""

from __future__ import annotations

from pathlib import Path

from flash_mineru.pipelines._shared.mineru_v4 import prepare_mineru_4_models

DEFAULT_MODEL_ROOT = "~/.mineru/models"


def prepare_mineru_4_standard(
    *,
    model: str | Path,
    download: bool = False,
    cache_dir: str | Path | None = None,
    small_backend: str = "torch",
    vlm_engine: str = "vllm",
    vlm_server_url: str = "",
    source: str | None = None,
) -> str:
    """Prepare the local models and MinerU 2.5 Pro VLM."""

    return prepare_mineru_4_models(
        model_root=cache_dir or model,
        tier="standard",
        download=download,
        small_backend=small_backend,
        vlm_engine=vlm_engine,
        vlm_server_url=vlm_server_url,
        source=source,
    )


__all__ = ["DEFAULT_MODEL_ROOT", "prepare_mineru_4_standard"]
