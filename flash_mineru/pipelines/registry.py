"""Small, explicit registry for the MinerU pipelines shipped by Flash-MinerU."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from rayorch import Pipeline


PrepareFn = Callable[..., str]


@dataclass(frozen=True, slots=True)
class PipelineSpec:
    """Everything needed to construct and prepare one supported pipeline."""

    name: str
    pipeline_cls: type[Pipeline]
    prepare: PrepareFn
    default_model: str
    runtime_requirement: str | None = None


@lru_cache(maxsize=1)
def _specs() -> dict[str, PipelineSpec]:
    # Imports stay local so ``import flash_mineru`` does not initialize vLLM,
    # Pillow, or any model downloader.
    from .mineru_v2_5.pipeline import MinerU25Pipeline
    from .mineru_v2_5.prepare import DEFAULT_MODEL, prepare_mineru_25
    from .mineru_v2_5_pro_2604.pipeline import MinerU25Pro2604Pipeline
    from .mineru_v2_5_pro_2604.prepare import (
        DEFAULT_MODEL as DEFAULT_MODEL_2604,
        prepare_mineru_25_pro_2604,
    )
    from .mineru_v2_5_pro_2605.pipeline import MinerU25Pro2605Pipeline
    from .mineru_v2_5_pro_2605.prepare import (
        DEFAULT_MODEL as DEFAULT_MODEL_2605,
        prepare_mineru_25_pro_2605,
    )
    from .mineru_v4_advanced.pipeline import MinerU4AdvancedPipeline
    from .mineru_v4_advanced.prepare import (
        DEFAULT_MODEL_ROOT as DEFAULT_V4_ADVANCED_MODEL_ROOT,
        prepare_mineru_4_advanced,
    )
    from .mineru_v4_advanced_local.pipeline import MinerU4AdvancedLocalPipeline
    from .mineru_v4_advanced_local.prepare import (
        DEFAULT_MODEL_ROOT as DEFAULT_V4_ADVANCED_LOCAL_MODEL_ROOT,
        prepare_mineru_4_advanced_local,
    )
    from .mineru_v4_advanced_shared.pipeline import (
        MinerU4AdvancedSharedPipeline,
    )
    from .mineru_v4_advanced_shared.prepare import (
        DEFAULT_MODEL_ROOT as DEFAULT_V4_ADVANCED_SHARED_MODEL_ROOT,
        prepare_mineru_4_advanced_shared,
    )
    from .mineru_v4_basic.pipeline import MinerU4BasicPipeline
    from .mineru_v4_basic.prepare import (
        DEFAULT_MODEL_ROOT as DEFAULT_V4_BASIC_MODEL_ROOT,
        prepare_mineru_4_basic,
    )
    from .mineru_v4_flash.pipeline import MinerU4FlashPipeline
    from .mineru_v4_flash.prepare import (
        DEFAULT_MODEL_ROOT as DEFAULT_V4_FLASH_MODEL_ROOT,
        prepare_mineru_4_flash,
    )
    from .mineru_v4_standard.pipeline import MinerU4StandardPipeline
    from .mineru_v4_standard.prepare import (
        DEFAULT_MODEL_ROOT as DEFAULT_V4_STANDARD_MODEL_ROOT,
        prepare_mineru_4_standard,
    )

    return {
        "v2.5": PipelineSpec(
            name="v2.5",
            pipeline_cls=MinerU25Pipeline,
            prepare=prepare_mineru_25,
            default_model=DEFAULT_MODEL,
        ),
        "v2.5-pro-2604": PipelineSpec(
            name="v2.5-pro-2604",
            pipeline_cls=MinerU25Pro2604Pipeline,
            prepare=prepare_mineru_25_pro_2604,
            default_model=DEFAULT_MODEL_2604,
            runtime_requirement="mineru>=3.1,<4",
        ),
        "v2.5-pro-2605": PipelineSpec(
            name="v2.5-pro-2605",
            pipeline_cls=MinerU25Pro2605Pipeline,
            prepare=prepare_mineru_25_pro_2605,
            default_model=DEFAULT_MODEL_2605,
            runtime_requirement="mineru>=3.3,<4",
        ),
        "v4-flash": PipelineSpec(
            name="v4-flash",
            pipeline_cls=MinerU4FlashPipeline,
            prepare=prepare_mineru_4_flash,
            default_model=DEFAULT_V4_FLASH_MODEL_ROOT,
            runtime_requirement="mineru>=4.0.4,<4.1",
        ),
        "v4-basic": PipelineSpec(
            name="v4-basic",
            pipeline_cls=MinerU4BasicPipeline,
            prepare=prepare_mineru_4_basic,
            default_model=DEFAULT_V4_BASIC_MODEL_ROOT,
            runtime_requirement="mineru>=4.0.4,<4.1",
        ),
        "v4-standard": PipelineSpec(
            name="v4-standard",
            pipeline_cls=MinerU4StandardPipeline,
            prepare=prepare_mineru_4_standard,
            default_model=DEFAULT_V4_STANDARD_MODEL_ROOT,
            runtime_requirement="mineru>=4.0.4,<4.1",
        ),
        "v4-advanced": PipelineSpec(
            name="v4-advanced",
            pipeline_cls=MinerU4AdvancedPipeline,
            prepare=prepare_mineru_4_advanced,
            default_model=DEFAULT_V4_ADVANCED_MODEL_ROOT,
            runtime_requirement="mineru>=4.0.4,<4.1",
        ),
        "v4-advanced-local": PipelineSpec(
            name="v4-advanced-local",
            pipeline_cls=MinerU4AdvancedLocalPipeline,
            prepare=prepare_mineru_4_advanced_local,
            default_model=DEFAULT_V4_ADVANCED_LOCAL_MODEL_ROOT,
            runtime_requirement="mineru>=4.0.4,<4.1",
        ),
        "v4-advanced-shared": PipelineSpec(
            name="v4-advanced-shared",
            pipeline_cls=MinerU4AdvancedSharedPipeline,
            prepare=prepare_mineru_4_advanced_shared,
            default_model=DEFAULT_V4_ADVANCED_SHARED_MODEL_ROOT,
            runtime_requirement="mineru>=4.0.4,<4.1",
        ),
    }


_ALIASES = {
    "2.5": "v2.5",
    "v2.5": "v2.5",
    "mineru2.5": "v2.5",
    "mineru-2.5": "v2.5",
    "2.5-pro-2604": "v2.5-pro-2604",
    "v2.5pro-2604": "v2.5-pro-2604",
    "2.5-pro-2605": "v2.5-pro-2605",
    "v2.5pro": "v2.5-pro-2605",
    "v2.5-pro": "v2.5-pro-2605",
    "v2.5pro-2605": "v2.5-pro-2605",
    "4-flash": "v4-flash",
    "mineru4-flash": "v4-flash",
    "4-basic": "v4-basic",
    "mineru4-basic": "v4-basic",
    "4-standard": "v4-standard",
    "mineru4-standard": "v4-standard",
    "4-advanced": "v4-advanced",
    "mineru4-advanced": "v4-advanced",
    "4-advanced-local": "v4-advanced-local",
    "mineru4-advanced-local": "v4-advanced-local",
    "4-advanced-shared": "v4-advanced-shared",
    "mineru4-advanced-shared": "v4-advanced-shared",
}


def pipeline_names() -> tuple[str, ...]:
    """Return the canonical names of bundled pipelines."""

    return tuple(_specs())


def get_pipeline_spec(version: str = "v2.5") -> PipelineSpec:
    """Resolve a public pipeline version without dynamic plugin discovery."""

    normalized = version.strip().lower()
    canonical = _ALIASES.get(normalized, normalized)
    try:
        return _specs()[canonical]
    except KeyError as exc:
        supported = ", ".join(pipeline_names())
        raise ValueError(
            f"unsupported MinerU pipeline {version!r}; supported: {supported}"
        ) from exc


def prepare_pipeline(
    version: str = "v2.5",
    *,
    model: str | Path | None = None,
    download: bool = False,
    cache_dir: str | Path | None = None,
    small_backend: str = "torch",
    vlm_engine: str = "vllm",
    vlm_server_url: str = "",
    source: str | None = None,
) -> str:
    """Resolve or download the model required by one registered pipeline."""

    spec = get_pipeline_spec(version)
    options: dict[str, Any] = dict(
        model=model or spec.default_model,
        download=download,
        cache_dir=cache_dir,
    )
    if spec.name.startswith("v4-"):
        options.update(
            small_backend=small_backend,
            vlm_engine=vlm_engine,
            vlm_server_url=vlm_server_url,
            source=source,
        )
    return spec.prepare(**options)


def create_pipeline(version: str = "v2.5", **kwargs: Any) -> Pipeline:
    """Construct one registered pipeline from explicit keyword arguments."""

    return get_pipeline_spec(version).pipeline_cls(**kwargs)


__all__ = [
    "PipelineSpec",
    "create_pipeline",
    "get_pipeline_spec",
    "pipeline_names",
    "prepare_pipeline",
]
