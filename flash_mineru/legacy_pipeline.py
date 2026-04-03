"""
Flash-MinerU **v0.0.4** sequential path: classic Ray ``ALL_SLICED_TO_ALL`` + batch-by-batch execution.

All code for this path lives in this module. It is **not** imported by :mod:`flash_mineru.main`
(the published API). Use :class:`MineruEngineLegacy` for benchmarks or migration only.

The default pipeline-parallel implementation is :mod:`flash_mineru.dag_pipeline`.
"""

from __future__ import annotations

import os
import warnings
from typing import List

from flash_mineru.ray_utils import Dispatch, RayModule
from flash_mineru.mineru_core.dispatch_mineru_class import (
    Convert2MDOp,
    Pdf2ImageOp,
    ProcessImagesOp,
)


def _abspaths(paths: List[str]) -> List[str]:
    return [os.path.abspath(p) for p in paths]


class SequentialRayPipeline:
    """Single PDF batch: pdf2img → VLM → Markdown (strict order, no cross-batch overlap)."""

    def __init__(
        self,
        *,
        model: str = "/home/dataset-local/models/MinerU2.5-2509-1.2B",
        replicas: int = 1,
        num_gpus_per_replica: float = 1.0,
        engine_gpu_util_rate_to_ray_cap: float = 0.9,
        save_dir: str = "outputs_mineru",
    ) -> None:
        self.save_dir = save_dir

        self.pdf2img = RayModule(
            Pdf2ImageOp,
            replicas=replicas,
            num_gpus_per_replica=0.0,
            dispatch_mode=Dispatch.ALL_SLICED_TO_ALL,
        ).pre_init()

        self.process_img = RayModule(
            ProcessImagesOp,
            replicas=replicas,
            num_gpus_per_replica=num_gpus_per_replica,
            dispatch_mode=Dispatch.ALL_SLICED_TO_ALL,
        ).pre_init(
            model=model,
            gpu_memory_utilization=engine_gpu_util_rate_to_ray_cap * num_gpus_per_replica,
        )

        self.img2md = RayModule(
            Convert2MDOp,
            replicas=replicas,
            num_gpus_per_replica=0.0,
            dispatch_mode=Dispatch.ALL_SLICED_TO_ALL,
        ).pre_init(output_dir=self.save_dir, parse_method="vlm")

    def run(self, pdf_paths: list[str]):
        print("Running MinerU Pipeline on data:", pdf_paths)
        images = self.pdf2img(pdf_paths)
        model_results = self.process_img(images)
        return self.img2md(model_results=model_results, images=images)


class MineruEngineLegacy:
    """Deprecated v0.0.4 engine: each logical batch runs :class:`SequentialRayPipeline` to completion before the next."""

    def __init__(
        self,
        *,
        model: str = "/home/dataset-local/models/MinerU2.5-2509-1.2B",
        save_dir: str = "outputs_mineru",
        batch_size: int = 4,
        replicas: int = 1,
        num_gpus_per_replica: float = 1,
        engine_gpu_util_rate_to_ray_cap: float = 0.9,
    ):
        warnings.warn(
            "MineruEngineLegacy is the deprecated v0.0.4 sequential API. "
            "Use flash_mineru.MineruEngine (v1.0.0, dag_pipeline) for the supported path.",
            DeprecationWarning,
            stacklevel=2,
        )
        import torch

        gpu_count = torch.cuda.device_count()

        assert replicas is None or num_gpus_per_replica is not None, (
            "Please specify num_gpus_per_replica when replicas is given."
        )
        assert replicas is None or num_gpus_per_replica * replicas <= gpu_count, (
            f"Too many gpus requested: num_gpus_per_replica * replicas = "
            f"{num_gpus_per_replica * replicas}, max allowed: {gpu_count}"
        )

        self._pipe = SequentialRayPipeline(
            model=model,
            save_dir=save_dir,
            replicas=replicas,
            num_gpus_per_replica=num_gpus_per_replica,
            engine_gpu_util_rate_to_ray_cap=engine_gpu_util_rate_to_ray_cap,
        )
        self.batch_size = batch_size

    def _check_path_exists(self, path_of_pdfs: list[str]) -> None:
        for path in path_of_pdfs:
            if not os.path.exists(path):
                raise FileNotFoundError(f"PDF file not found: {path}")

    def run(self, path_of_pdfs: list[str]) -> list:
        print("MineruEngineLegacy is running... for ", path_of_pdfs)
        self._check_path_exists(path_of_pdfs)
        path_of_pdfs = _abspaths(path_of_pdfs)

        steps = (len(path_of_pdfs) + self.batch_size - 1) // self.batch_size
        results = []
        for step in range(steps):
            batch_paths = path_of_pdfs[
                step * self.batch_size : (step + 1) * self.batch_size
            ]
            results.append(self._pipe.run(batch_paths))
            print(f"MineruEngineLegacy finished batch {step + 1}/{steps}")

        print("MineruEngineLegacy finished.")
        return results
