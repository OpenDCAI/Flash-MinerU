# Flash-MinerU ⚡️📄

<div align="center">

[![](https://img.shields.io/github/stars/OpenDCAI/Flash-MinerU?style=social)](https://github.com/OpenDCAI/Flash-MinerU)
[![](https://img.shields.io/github/issues-raw/OpenDCAI/Flash-MinerU)](https://github.com/OpenDCAI/Flash-MinerU/issues)
[![issue resolution](https://img.shields.io/github/issues-closed-raw/OpenDCAI/Flash-MinerU)](https://github.com/OpenDCAI/Flash-MinerU/issues?q=is%3Aissue%20state%3Aclosed)
[![](https://img.shields.io/github/issues-pr-raw/OpenDCAI/Flash-MinerU)](https://github.com/OpenDCAI/Flash-MinerU/pulls)
[![pr resolution](https://img.shields.io/github/issues-pr-closed-raw/OpenDCAI/Flash-MinerU)](https://github.com/OpenDCAI/Flash-MinerU/pulls?q=is%3Apr+is%3Aclosed)
[![](https://img.shields.io/github/contributors/OpenDCAI/Flash-MinerU)](https://github.com/OpenDCAI/Flash-MinerU/graphs/contributors)
[![](https://img.shields.io/github/repo-size/OpenDCAI/Flash-MinerU?color=green)](https://github.com/OpenDCAI/Flash-MinerU)


[![PyPI version](https://img.shields.io/pypi/v/flash-mineru)](https://pypi.org/project/flash-mineru/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/flash-mineru)](https://pypi.org/project/flash-mineru/)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/flash-mineru?style=flat&logo=python)](https://pypistats.org/packages/flash-mineru)
[![Downloads](https://static.pepy.tech/badge/flash-mineru)](https://pepy.tech/project/flash-mineru)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/OpenDCAI/Flash-MinerU)

[简体中文](./README-zh.md) | English

</div>



> Accelerating the **VLM Inference Pipeline** of the open-source PDF parsing project **MinerU** with **Ray**

Flash-MinerU is a **lightweight and low-intrusion** acceleration project. Its goal is to leverage **Ray’s parallel and distributed capabilities** to parallelize and accelerate the most time-consuming stage in **MinerU** — the **VLM (Vision-Language Model) inference stage** — thereby significantly improving the overall throughput of **PDF → Markdown** processing.

This project is positioned as a **parallelization and engineering accelerator**, rather than a reimplementation of MinerU’s core algorithms. Its design goals include:

- **Minimal dependencies, lightweight installation**
  - One-click install & run via `pip install flash-mineru`
  - Tested in **domestic computing environments such as METAX**
- **Maximum reuse of MinerU’s original logic and data structures**
  - Preserving algorithmic behavior and output consistency
- **Multi-GPU / multi-process / multi-cluster friendly**
  - Designed for large-scale batch PDF processing, easy to scale up

---

## ✨ Features

- 🚀 **Ray-based parallel inference**  
  PDF pages / images are sliced into batches and dispatched to multiple Ray actors for parallel execution

- 🧠 **VLM inference acceleration**  
  Focuses on the VLM inference stage in MinerU; currently defaults to **vLLM** for high-throughput inference

- 🧩 **Low-intrusion design**  
  Retains MinerU’s original intermediate structures (`middle_json`) and Markdown generation logic

---

## 📦 Installation

### Basic installation (lightweight mode)

Suitable if you have **already installed the inference backend manually** (e.g., vLLM), or are using an image with a prebuilt environment:

```bash
git clone https://github.com/OpenDCAI/Flash-MinerU.git
cd Flash-MinerU
pip install -e .
```

### Install with vLLM backend enabled (optional)

If you want Flash-MinerU to install vLLM as the inference backend for you:

```bash
git clone https://github.com/OpenDCAI/Flash-MinerU.git
cd Flash-MinerU
pip install -e ".[vllm]"
```

---

## 🚀 Quickstart

### Minimal Python API example

```python
import ray
from flash_mineru import MineruEngine

ray.init()  # or ray.init(address="auto")

pdfs = [
    "resnet.pdf",
    "yolo.pdf",
    "text2sql.pdf",
]

engine = MineruEngine(
    model="<path_to_local>/MinerU2.5-2509-1.2B",
    # Model can be downloaded from https://huggingface.co/opendatalab/MinerU2.5-2509-1.2B
    batch_size=1,              # Number of PDFs processed concurrently per model instance
    replicas=3,                # Number of parallel vLLM / model instances
    num_gpus_per_replica=0.5, # Fraction of GPU memory used per instance (vLLM KV cache)
    save_dir="outputs_mineru", # Output directory for parsed results
)

results = engine.run(pdfs)
print(results)  # list[list[str]]
```

### Output structure

* Each PDF’s parsing results will be generated under:

  ```
  <save_dir>/<pdf_name>/
  ```

* The Markdown file is located by default at:

  ```
  <save_dir>/<pdf_name>/vlm/<pdf_name>.md
  ```

---

## 🗺️ Roadmap

* [ ] Benchmark scripts (single GPU vs multiple replicas)
* [ ] Support for more inference backends (e.g., sglang)
* [ ] Service-oriented deployment (HTTP API / task queue)
* [ ] Sample datasets and more comprehensive documentation

---

## 🤝 Acknowledgements

* **MinerU**
  This project is built upon MinerU’s overall algorithm design and engineering practices, and parallelizes its VLM inference pipeline.
  The `mineru_core/` directory contains code logic copied from and adapted to the MinerU project.
  We extend our sincere respect and gratitude to the original authors and all contributors of MinerU.
  🔗 Official repository / homepage:
  [https://github.com/opendatalab/MinerU](https://github.com/opendatalab/MinerU)

* **Ray**
  Provides powerful abstractions for distributed and parallel computing, making multi-GPU and multi-process orchestration simpler and more reliable.
  🔗 Official website:
  [https://www.ray.io/](https://www.ray.io/)
  🔗 Official GitHub:
  [https://github.com/ray-project/ray](https://github.com/ray-project/ray)

* **vLLM**
  Provides a high-throughput, production-ready inference engine (currently the default backend).
  🔗 Official website:
  [https://vllm.ai/](https://vllm.ai/)
  🔗 Official GitHub:
  [https://github.com/vllm-project/vllm](https://github.com/vllm-project/vllm)

---

## 📜 License

**AGPL-3.0**

> Notes:
> The `mineru_core/` directory in this project contains derivative code based on **MinerU (AGPL-3.0)**.
> In accordance with the AGPL-3.0 license requirements, this repository as a whole is released under **AGPL-3.0** as a derivative work.
> For details, please refer to the root `LICENSE` file and `mineru_core/README.md`.

