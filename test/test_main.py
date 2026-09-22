"""Manual smoke test.

Run with a real model and GPU:

    FLASH_MINERU_MODEL=/path/to/MinerU2.5-2509-1.2B \
      python test/test_main.py

Keeping execution behind ``main`` makes normal test discovery side-effect
free: importing this module never starts Ray or loads a model.
"""

from __future__ import annotations

import os
import time
from pathlib import Path

from flash_mineru import MineruEngine


def main() -> None:
    model = os.environ.get("FLASH_MINERU_MODEL")
    if not model:
        raise SystemExit("set FLASH_MINERU_MODEL to a local MinerU 2.5 model")

    data_dir = Path("test/sample_pdfs")
    pdfs = sorted(str(path) for path in data_dir.glob("*.pdf"))
    if not pdfs:
        raise SystemExit(f"no PDFs found under {data_dir}")

    start_time = time.perf_counter()
    with MineruEngine(
        pipeline_version="v2.5",
        model=model,
        batch_size=2,
        replicas=3,
        num_gpus_per_replica=0.9,
        save_dir="outputs_mineru",
        inflight=4,
    ) as engine:
        results = engine.run(pdfs)

    print("Final Result:")
    print(results)
    print(f"Total time taken: {time.perf_counter() - start_time} seconds")


if __name__ == "__main__":
    main()
