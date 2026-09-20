"""Flash-MinerU pipeline implemented with the installed RayOrch package."""

from __future__ import annotations

import os
import warnings
from pathlib import Path
from typing import Any

from rayorch import Executor, Pipeline, RayModule

from flash_mineru.mineru_core.dispatch_mineru_class import (
    Convert2MDOp,
    Pdf2ImageOp,
    ProcessImagesOp,
)


class FlashMinerRayOrchPipeline(Pipeline):
    """Declarative PDF rendering, VLM inference, and Markdown pipeline."""

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
        runtime_env: dict[str, Any] | None = None,
    ) -> None:
        del inflight  # Input-batch overlap is configured on Executor.run().
        if dev_mode:
            warnings.warn(
                "dev_mode is retained for API compatibility, but RayOrch 0.1.0 "
                "does not expose Flash-MinerU's former actor-level NVTX ranges.",
                DeprecationWarning,
                stacklevel=2,
            )
        actor_batch_size = max(1, (batch_size + replicas - 1) // replicas)
        common_options: dict[str, Any] = {
            "replicas": replicas,
            "batch_size": actor_batch_size,
        }
        if runtime_env is not None:
            common_options["runtime_env"] = dict(runtime_env)

        self.pdf2img = RayModule(Pdf2ImageOp).ray_options(
            **common_options,
            num_gpus=0.0,
        )
        self.process_img = (
            RayModule(ProcessImagesOp)
            .pre_init(
                model=model,
                gpu_memory_utilization=(
                    engine_gpu_util_rate_to_ray_cap * num_gpus_per_replica
                ),
            )
            .ray_options(
                **common_options,
                num_gpus=num_gpus_per_replica,
            )
        )
        self.img2md = (
            RayModule(Convert2MDOp)
            .pre_init(output_dir=save_dir, parse_method="vlm")
            .ray_options(
                **common_options,
                num_gpus=0.0,
            )
        )

    def forward(self, pdf_paths):
        images = self.pdf2img(pdf_paths)
        model_results = self.process_img(images)
        return self.img2md(model_results, images)


class MineruRayOrchDagEngine:
    """Run Flash-MinerU with persistent actors from installed RayOrch."""

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
        runtime_env: dict[str, Any] | None = None,
    ) -> None:
        if type(batch_size) is not int or batch_size <= 0:
            raise ValueError("batch_size must be a positive integer")
        if type(replicas) is not int or replicas <= 0:
            raise ValueError("replicas must be a positive integer")
        if num_gpus_per_replica < 0:
            raise ValueError("num_gpus_per_replica cannot be negative")
        if engine_gpu_util_rate_to_ray_cap <= 0:
            raise ValueError("engine_gpu_util_rate_to_ray_cap must be positive")

        self.batch_size = batch_size
        self._inflight = max(1, int(inflight))
        self._log_label = log_label
        self.dev_mode = dev_mode  # Kept for API compatibility.
        pipeline = FlashMinerRayOrchPipeline(
            model=str(Path(model).expanduser().resolve()),
            replicas=replicas,
            num_gpus_per_replica=num_gpus_per_replica,
            save_dir=save_dir,
            engine_gpu_util_rate_to_ray_cap=engine_gpu_util_rate_to_ray_cap,
            dev_mode=dev_mode,
            batch_size=batch_size,
            runtime_env=runtime_env,
        )
        # An engine may process many calls, so reuse one Executor and its loaded models.
        # Pipeline.run() remains the concise choice for one-shot applications.
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
            input_batch_size=self.batch_size,
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
