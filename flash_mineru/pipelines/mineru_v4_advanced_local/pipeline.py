"""RayOrch graph for one complete MinerU 4 Advanced stack per GPU."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

from rayorch import F, Pipeline, Port, RayModule

from .udfs import (
    MinerU4AdvancedLocalAnalyzeWindow,
    MinerU4AdvancedLocalAssembleDocument,
    MinerU4AdvancedLocalRenderWindow,
    MinerU4AdvancedLocalSplitPdf,
)

_STAGES = ("split", "render", "analyze", "assemble")


class MinerU4AdvancedLocalPipeline(Pipeline):
    """Run one local Layout + Two-Step VLM + MFR + OCR stack on each GPU."""

    def __init__(
        self,
        *,
        output_dir: str,
        model: str,
        replicas: int = 1,
        ocr_batch_size: int = 8,
        num_gpus_per_replica: float = 1.0,
        gpu_memory_utilization: float = 0.1,
        render_dpi: int = 200,
        start_page_id: int = 0,
        end_page_id: int | None = None,
        image_analysis: bool = True,
        parse_mode: str = "auto",
        window_size: int = 8,
        layout_batch_size: int = 8,
        small_backend: str = "torch",
        vlm_engine: str = "vllm",
        vlm_server_url: str = "",
        vlm_api_key: str = "",
        vlm_model: str = "",
        vlm_max_concurrency: int = 100,
        runtime_env: dict[str, Any] | None = None,
        stage_options: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> None:
        if render_dpi != 200 or start_page_id != 0 or end_page_id is not None:
            raise ValueError(
                "MinerU 4 pipelines currently require render_dpi=200 and the "
                "full PDF page range"
            )
        if vlm_server_url:
            raise ValueError(
                "v4-advanced-local requires an actor-local VLM; "
                "vlm_server_url must be empty"
            )
        if vlm_engine != "vllm":
            raise ValueError(
                "v4-advanced-local currently requires vlm_engine='vllm'"
            )
        if not 0 < gpu_memory_utilization < 1:
            raise ValueError("gpu_memory_utilization must be between 0 and 1")

        options = _normalize_stage_options(stage_options)
        common = {"runtime_env": dict(runtime_env)} if runtime_env else {}
        self.split = (
            RayModule(MinerU4AdvancedLocalSplitPdf)
            .pre_init(window_size=window_size, parse_mode=parse_mode)
            .ray_options(
                **_merge(
                    {**common, "replicas": replicas, "batch_size": 1, "num_cpus": 1},
                    options.get("split", {}),
                )
            )
        )
        self.render = (
            RayModule(MinerU4AdvancedLocalRenderWindow)
            .pre_init(render_dpi=render_dpi)
            .ray_options(
                **_merge(
                    {
                        **common,
                        "replicas": replicas,
                        "batch_size": 1,
                        "num_cpus": 1,
                    },
                    options.get("render", {}),
                )
            )
        )
        self.analyze = (
            RayModule(MinerU4AdvancedLocalAnalyzeWindow)
            .pre_init(
                model_base_dir=model,
                small_backend=small_backend,
                vlm_engine="vllm",
                vlm_server_url="",
                vlm_api_key=vlm_api_key,
                vlm_model=vlm_model,
                vlm_max_concurrency=vlm_max_concurrency,
                gpu_memory_utilization=gpu_memory_utilization,
                image_analysis=image_analysis,
                rendered_input=True,
                layout_batch_size=layout_batch_size,
            )
            .ray_options(
                **_merge(
                    {
                        **common,
                        "replicas": replicas,
                        "batch_size": ocr_batch_size,
                        "num_gpus": num_gpus_per_replica,
                        "num_cpus": 1,
                    },
                    options.get("analyze", {}),
                )
            )
        )
        self.assemble = (
            RayModule(MinerU4AdvancedLocalAssembleDocument)
            .pre_init(output_dir=output_dir)
            .ray_options(
                **_merge(
                    {**common, "replicas": replicas, "batch_size": 4, "num_cpus": 1},
                    options.get("assemble", {}),
                )
            )
        )

    def forward(self, pdfs: Port):
        windows = F.expand(cast(Port, self.split(pdfs)))
        rendered = cast(Port, self.render(windows))
        analyzed = cast(Port, self.analyze(rendered))
        return self.assemble(F.reduce(analyzed), pdfs)


def _normalize_stage_options(
    value: Mapping[str, Mapping[str, Any]] | None,
) -> dict[str, dict[str, Any]]:
    if value is None:
        return {}
    unknown = sorted(set(value) - set(_STAGES))
    if unknown:
        raise ValueError(f"unknown MinerU 4 stage {unknown[0]!r}")
    return {stage: dict(overrides) for stage, overrides in value.items()}


def _merge(
    defaults: Mapping[str, Any], overrides: Mapping[str, Any]
) -> dict[str, Any]:
    return {**defaults, **overrides}


__all__ = ["MinerU4AdvancedLocalPipeline"]
