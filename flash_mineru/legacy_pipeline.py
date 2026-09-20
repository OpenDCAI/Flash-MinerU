"""Compatibility wrapper for Flash-MinerU's former sequential API."""

from __future__ import annotations

import os
import warnings

from rayorch import Executor

from .dag_pipeline import FlashMinerRayOrchPipeline


class SequentialRayPipeline:
    """Run one PDF batch at a time through the installed RayOrch package."""

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
        self._pipeline = FlashMinerRayOrchPipeline(
            model=model,
            replicas=replicas,
            num_gpus_per_replica=num_gpus_per_replica,
            save_dir=save_dir,
            engine_gpu_util_rate_to_ray_cap=engine_gpu_util_rate_to_ray_cap,
            batch_size=replicas,
        )
        self.pdf2img = self._pipeline.pdf2img
        self.process_img = self._pipeline.process_img
        self.img2md = self._pipeline.img2md
        self._executor = Executor(self._pipeline)

    def run(self, pdf_paths: list[str]) -> list[str]:
        print("Running MinerU Pipeline on data:", pdf_paths)
        return list(
            self._executor.run(
                pdf_paths,
                input_batch_size=max(1, len(pdf_paths)),
                max_active_input_batches=1,
            ).outputs
        )

    def close(self) -> None:
        self._executor.close()


class MineruEngineLegacy:
    """Deprecated sequential-batch API backed by installed RayOrch."""

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
            "MineruEngineLegacy is deprecated; use flash_mineru.MineruEngine.",
            DeprecationWarning,
            stacklevel=2,
        )
        if type(batch_size) is not int or batch_size <= 0:
            raise ValueError("batch_size must be a positive integer")
        self._pipe = SequentialRayPipeline(
            model=model,
            save_dir=save_dir,
            replicas=replicas,
            num_gpus_per_replica=num_gpus_per_replica,
            engine_gpu_util_rate_to_ray_cap=engine_gpu_util_rate_to_ray_cap,
        )
        self.batch_size = batch_size

    @staticmethod
    def _check_path_exists(path_of_pdfs: list[str]) -> None:
        for path in path_of_pdfs:
            if not os.path.exists(path):
                raise FileNotFoundError(f"PDF file not found: {path}")

    def run(self, path_of_pdfs: list[str]) -> list[list[str]]:
        print("MineruEngineLegacy is running... for ", path_of_pdfs)
        self._check_path_exists(path_of_pdfs)
        pdfs = [os.path.abspath(path) for path in path_of_pdfs]
        results = []
        for start in range(0, len(pdfs), self.batch_size):
            results.append(self._pipe.run(pdfs[start : start + self.batch_size]))
            print(
                "MineruEngineLegacy finished batch "
                f"{len(results)}/{(len(pdfs) + self.batch_size - 1) // self.batch_size}"
            )
        print("MineruEngineLegacy finished.")
        return results

    def close(self) -> None:
        self._pipe.close()


__all__ = ["MineruEngineLegacy", "SequentialRayPipeline"]
