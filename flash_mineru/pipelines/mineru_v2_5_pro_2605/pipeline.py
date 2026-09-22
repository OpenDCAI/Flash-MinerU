"""RayOrch graph for MinerU 2.5 Pro 2605."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

from rayorch import F, Pipeline, Port, RayModule

from .udfs import (
    MinerU25Pro2605AssembleDocument,
    MinerU25Pro2605PdfStem,
    MinerU25Pro2605PdfToPages,
    MinerU25Pro2605VlmPage,
)

_STAGES = ("render", "ocr", "metadata", "assemble")


class MinerU25Pro2605Pipeline(Pipeline):
    """Page-grained MinerU 2.5 Pro 2605 graph."""

    def __init__(
        self,
        *,
        output_dir: str,
        model: str,
        replicas: int = 1,
        ocr_batch_size: int = 128,
        num_gpus_per_replica: float = 1.0,
        gpu_memory_utilization: float = 0.9,
        render_dpi: int = 200,
        start_page_id: int = 0,
        end_page_id: int | None = None,
        image_analysis: bool = True,
        runtime_env: dict[str, Any] | None = None,
        stage_options: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> None:
        options = _normalize_stage_options(stage_options)
        common = {"runtime_env": dict(runtime_env)} if runtime_env else {}
        self.render = (
            RayModule(MinerU25Pro2605PdfToPages)
            .pre_init(
                dpi=render_dpi,
                start_page_id=start_page_id,
                end_page_id=end_page_id,
            )
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
        self.ocr = (
            RayModule(MinerU25Pro2605VlmPage)
            .pre_init(
                model=model,
                gpu_memory_utilization=gpu_memory_utilization,
                image_analysis=image_analysis,
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
                    options.get("ocr", {}),
                )
            )
        )
        self.metadata = RayModule(MinerU25Pro2605PdfStem).ray_options(
            **_merge(
                {
                    **common,
                    "replicas": 1,
                    "batch_size": 32,
                    "num_cpus": 1,
                },
                options.get("metadata", {}),
            )
        )
        self.assemble = (
            RayModule(MinerU25Pro2605AssembleDocument)
            .pre_init(output_dir=output_dir)
            .ray_options(
                **_merge(
                    {
                        **common,
                        "replicas": replicas,
                        "batch_size": 4,
                        "num_cpus": 1,
                    },
                    options.get("assemble", {}),
                )
            )
        )

    def forward(self, pdfs: Port):
        pages = F.expand(cast(Port, self.render(pdfs)))
        contents = cast(Port, self.ocr(pages))
        stems = cast(Port, self.metadata(pdfs))
        content_groups, page_groups = F.reduce_aligned(
            contents,
            pages,
            members=contents,
        )
        return self.assemble(content_groups, page_groups, stems)


def _normalize_stage_options(
    value: Mapping[str, Mapping[str, Any]] | None,
) -> dict[str, dict[str, Any]]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise TypeError("stage_options must be a mapping")
    unknown = sorted(set(value) - set(_STAGES))
    if unknown:
        raise ValueError(
            f"unknown MinerU stage {unknown[0]!r}; expected one of "
            f"{', '.join(_STAGES)}"
        )
    normalized = {}
    for stage, overrides in value.items():
        if not isinstance(overrides, Mapping):
            raise TypeError(f"stage_options[{stage!r}] must be a mapping")
        if "num_outputs" in overrides:
            raise ValueError("stage_options cannot override num_outputs")
        normalized[stage] = dict(overrides)
    return normalized


def _merge(
    defaults: Mapping[str, Any],
    overrides: Mapping[str, Any],
) -> dict[str, Any]:
    return {**defaults, **overrides}


__all__ = ["MinerU25Pro2605Pipeline"]
