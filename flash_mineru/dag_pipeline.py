"""Compatibility engine backed by a versioned Flash-MinerU pipeline."""

from __future__ import annotations

import os
import warnings
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from rayorch import Executor

from flash_mineru.pipelines.registry import (
    create_pipeline,
    get_pipeline_spec,
    prepare_pipeline,
)
from flash_mineru.pipelines.mineru_v2_5.pipeline import MinerU25Pipeline


class FlashMinerRayOrchPipeline(MinerU25Pipeline):
    """Backward-compatible name for the default MinerU 2.5 pipeline."""

    def __init__(
        self,
        *,
        model: str,
        replicas: int,
        num_gpus_per_replica: float,
        save_dir: str,
        engine_gpu_util_rate_to_ray_cap: float,
        inflight: int = 4,
        dev_mode: bool = False,
        batch_size: int = 64,
        ocr_batch_size: int = 128,
        render_dpi: int = 200,
        start_page_id: int = 0,
        end_page_id: int | None = None,
        runtime_env: dict[str, Any] | None = None,
        stage_options: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> None:
        del inflight
        if dev_mode:
            warnings.warn(
                "dev_mode is retained for API compatibility, but RayOrch 0.1 "
                "does not expose Flash-MinerU's former actor-level NVTX ranges.",
                DeprecationWarning,
                stacklevel=2,
            )
        del batch_size  # Historical PDF result grouping is an engine concern.
        super().__init__(
            output_dir=save_dir,
            model=model,
            replicas=replicas,
            ocr_batch_size=ocr_batch_size,
            num_gpus_per_replica=num_gpus_per_replica,
            gpu_memory_utilization=(
                engine_gpu_util_rate_to_ray_cap * num_gpus_per_replica
            ),
            render_dpi=render_dpi,
            start_page_id=start_page_id,
            end_page_id=end_page_id,
            runtime_env=runtime_env,
            stage_options=stage_options,
        )

    @property
    def pdf2img(self):
        """Compatibility alias for the former rendering stage name."""

        return self.render

    @property
    def process_img(self):
        """Compatibility alias for the former VLM stage name."""

        return self.ocr

    @property
    def img2md(self):
        """Compatibility alias for the former assembly stage name."""

        return self.assemble


class MineruRayOrchDagEngine:
    """Run a named MinerU pipeline while preserving Flash-MinerU's public API."""

    def __init__(
        self,
        *,
        model: str | None = None,
        save_dir: str,
        batch_size: int,
        replicas: int,
        num_gpus_per_replica: float = 1.0,
        engine_gpu_util_rate_to_ray_cap: float | None = None,
        inflight: int = 4,
        dev_mode: bool = False,
        log_label: str = "MineruRayOrchDagEngine",
        runtime_env: dict[str, Any] | None = None,
        pipeline_version: str = "v2.5",
        prepare: bool = False,
        model_cache_dir: str | None = None,
        model_source: str | None = None,
        ocr_batch_size: int | None = None,
        input_batch_size: int = 24,
        render_dpi: int = 200,
        start_page_id: int = 0,
        end_page_id: int | None = None,
        stage_options: Mapping[str, Mapping[str, Any]] | None = None,
        image_analysis: bool | None = None,
        parse_mode: str = "auto",
        window_size: int = 8,
        layout_batch_size: int = 8,
        small_backend: str = "torch",
        vlm_engine: str = "vllm",
        vlm_server_url: str = "",
        vlm_api_key: str = "",
        vlm_model: str = "",
        vlm_max_concurrency: int = 100,
    ) -> None:
        if type(batch_size) is not int or batch_size <= 0:
            raise ValueError("batch_size must be a positive integer")
        if type(replicas) is not int or replicas <= 0:
            raise ValueError("replicas must be a positive integer")
        if type(input_batch_size) is not int or input_batch_size <= 0:
            raise ValueError("input_batch_size must be a positive integer")
        if num_gpus_per_replica < 0:
            raise ValueError("num_gpus_per_replica cannot be negative")
        spec = get_pipeline_spec(pipeline_version)
        if engine_gpu_util_rate_to_ray_cap is None:
            # A local Advanced actor also hosts Layout, MFR, and OCR beside
            # vLLM. Use the validated conservative default so the concise API
            # works without requiring users to discover this distinction.
            engine_gpu_util_rate_to_ray_cap = (
                0.1
                if spec.name in {"v4-advanced-local", "v4-advanced-shared"}
                else 0.9
            )
        if engine_gpu_util_rate_to_ray_cap <= 0:
            raise ValueError("engine_gpu_util_rate_to_ray_cap must be positive")
        if ocr_batch_size is None:
            if spec.name == "v4-advanced-shared":
                ocr_batch_size = 16
            elif spec.name == "v4-basic":
                # Basic now batches the local formula/table/OCR stages across
                # ready windows. Keep the default large enough to expose that
                # benefit without collecting an unbounded number of rendered
                # pages and OCR crops in one actor call.
                ocr_batch_size = 32
            elif spec.name.startswith("v4-"):
                ocr_batch_size = 8
            else:
                ocr_batch_size = 128
        if type(ocr_batch_size) is not int or ocr_batch_size <= 0:
            raise ValueError("ocr_batch_size must be a positive integer")
        model_path = prepare_pipeline(
            spec.name,
            model=model,
            download=prepare,
            cache_dir=model_cache_dir,
            small_backend=small_backend,
            vlm_engine=vlm_engine,
            vlm_server_url=vlm_server_url,
            source=model_source,
        )
        self.batch_size = batch_size
        self.pipeline_version = spec.name
        self._input_batch_size = input_batch_size
        self._inflight = max(1, int(inflight))
        self._log_label = log_label
        self.dev_mode = dev_mode
        if image_analysis is None:
            image_analysis = spec.name in {
                "v2.5-pro-2604",
                "v2.5-pro-2605",
                "v4-standard",
                "v4-advanced",
                "v4-advanced-local",
                "v4-advanced-shared",
            }
        pipeline_kwargs = dict(
            output_dir=save_dir,
            model=str(Path(model_path).expanduser().resolve()),
            replicas=replicas,
            num_gpus_per_replica=num_gpus_per_replica,
            ocr_batch_size=ocr_batch_size,
            gpu_memory_utilization=(
                engine_gpu_util_rate_to_ray_cap * num_gpus_per_replica
            ),
            render_dpi=render_dpi,
            start_page_id=start_page_id,
            end_page_id=end_page_id,
            runtime_env=runtime_env,
            stage_options=stage_options,
            image_analysis=image_analysis,
        )
        if spec.name.startswith("v4-"):
            pipeline_kwargs.update(
                parse_mode=parse_mode,
                window_size=window_size,
                small_backend=small_backend,
                vlm_engine=vlm_engine,
                vlm_server_url=vlm_server_url,
                vlm_api_key=vlm_api_key,
                vlm_model=vlm_model,
                vlm_max_concurrency=vlm_max_concurrency,
            )
            # Flash mode has no separate batched Layout stage. Keep its
            # constructor honest instead of passing an unused tuning option.
            if spec.name != "v4-flash":
                pipeline_kwargs["layout_batch_size"] = layout_batch_size
        pipeline = create_pipeline(spec.name, **pipeline_kwargs)
        self._executor = Executor(pipeline)

    @staticmethod
    def _check_path_exists(path_of_pdfs: list[str]) -> None:
        for path in path_of_pdfs:
            if not os.path.exists(path):
                raise FileNotFoundError(f"PDF file not found: {path}")

    def run(self, path_of_pdfs: list[str]) -> list[list[str]]:
        """Process PDFs and preserve the historical batch-grouped result."""

        print(f"{self._log_label} is running... for ", path_of_pdfs)
        self._check_path_exists(path_of_pdfs)
        pdfs = [os.path.abspath(path) for path in path_of_pdfs]
        if not pdfs:
            return []

        result = self._executor.run(
            pdfs,
            input_batch_size=self._input_batch_size,
            max_active_input_batches=self._inflight,
        )
        outputs = list(result.outputs)
        batches = [
            outputs[start : start + self.batch_size]
            for start in range(0, len(outputs), self.batch_size)
        ]
        for index in range(len(batches)):
            print(f"{self._log_label} finished batch {index + 1}/{len(batches)}")
        print(f"{self._log_label} finished.")
        return batches

    def close(self) -> None:
        """Release persistent Ray actors owned by this engine."""

        self._executor.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()


__all__ = ["FlashMinerRayOrchPipeline", "MineruRayOrchDagEngine"]
