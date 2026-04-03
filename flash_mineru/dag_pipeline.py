"""
Flash-MinerU **v1.0.0** DAG / pipeline-parallel path (RayOrch ``Pipeline`` + ``DagExecutor``).

All code for this path lives in this module. The published :class:`~flash_mineru.main.MineruEngine`
is a thin wrapper around :class:`MineruRayOrchDagEngine` with user-facing log labels.

The older sequential Ray path is in :mod:`flash_mineru.legacy_pipeline`.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import List

from flash_mineru.ray_utils.rayorch_runtime import (
    DagExecutor,
    DagNewPipeRef as PipeRef,
    Dispatch,
    Pipeline,
    RayModule as OrchRayModule,
)

from flash_mineru.mineru_core.dispatch_mineru_class import (
    Convert2MDOp,
    Pdf2ImageOp,
    ProcessImagesOp,
)


class FlashMinerRayOrchPipeline(Pipeline):
    """Declarative graph: pdf2img → process_img → img2md (same ops as :class:`legacy_pipeline.SequentialRayPipeline`)."""

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
    ) -> None:
        r = max(1, int(replicas))
        cap = max(1, int(inflight))
        util = engine_gpu_util_rate_to_ray_cap * num_gpus_per_replica

        self.pdf2img = OrchRayModule(
            Pdf2ImageOp,
            replicas=r,
            num_gpus_per_replica=0.0,
            dispatch_mode=Dispatch.SHARD_CONTIGUOUS,
            max_inflight=cap,
            dev_mode=dev_mode,
        ).pre_init()

        self.process_img = OrchRayModule(
            ProcessImagesOp,
            replicas=r,
            num_gpus_per_replica=num_gpus_per_replica,
            dispatch_mode=Dispatch.SHARD_CONTIGUOUS,
            max_inflight=cap,
            dev_mode=dev_mode,
        ).pre_init(model=model, gpu_memory_utilization=util)

        self.img2md = OrchRayModule(
            Convert2MDOp,
            replicas=r,
            num_gpus_per_replica=0.0,
            dispatch_mode=Dispatch.SHARD_CONTIGUOUS,
            max_inflight=cap,
            dev_mode=dev_mode,
        ).pre_init(output_dir=save_dir, parse_method="vlm")

        super().__init__()

    def forward(self, x: PipeRef) -> PipeRef:
        images = self.pdf2img(x)
        model_results = self.process_img(images)
        return self.img2md(model_results, images)


class MineruRayOrchDagEngine:
    """Chunk PDFs, run overlapped batches through :class:`FlashMinerRayOrchPipeline`, return one list per logical batch."""

    def __init__(
        self,
        *,
        model: str,
        save_dir: str,
        batch_size: int,
        replicas: int,
        num_gpus_per_replica: float = 1.0,
        engine_gpu_util_rate_to_ray_cap: float = 0.9,
        inflight: int = 4,
        dev_mode: bool = False,
        log_label: str = "MineruRayOrchDagEngine",
    ) -> None:
        import torch

        gpu_count = torch.cuda.device_count()
        need = num_gpus_per_replica * replicas
        if need > gpu_count:
            raise AssertionError(
                f"Too many GPUs requested: num_gpus_per_replica * replicas = {need}, "
                f"available: {gpu_count}"
            )

        self.batch_size = batch_size
        self._inflight = max(1, int(inflight))
        self._log_label = log_label
        model_resolved = str(Path(model).expanduser().resolve())
        self._pipe = FlashMinerRayOrchPipeline(
            model=model_resolved,
            replicas=replicas,
            num_gpus_per_replica=num_gpus_per_replica,
            save_dir=save_dir,
            engine_gpu_util_rate_to_ray_cap=engine_gpu_util_rate_to_ray_cap,
            inflight=self._inflight,
            dev_mode=dev_mode,
        )
        self._pipe.compile()

    def _check_path_exists(self, path_of_pdfs: list[str]) -> None:
        for path in path_of_pdfs:
            if not os.path.exists(path):
                raise FileNotFoundError(f"PDF file not found: {path}")

    def run(self, path_of_pdfs: list[str]) -> list:
        print(f"{self._log_label} is running... for ", path_of_pdfs)
        self._check_path_exists(path_of_pdfs)
        path_of_pdfs = [os.path.abspath(p) for p in path_of_pdfs]

        bs = self.batch_size
        steps = (len(path_of_pdfs) + bs - 1) // bs
        batches: List[List[str]] = [
            path_of_pdfs[i * bs : (i + 1) * bs] for i in range(steps)
        ]

        executor = DagExecutor(max_batches_inflight=self._inflight)
        results = executor.run(self._pipe, batches)

        for i in range(len(results)):
            print(f"{self._log_label} finished batch {i + 1}/{len(results)}")
        print(f"{self._log_label} finished.")
        return results
