# Flash-MinerU ⚡️📄

<div align="center">
<img width="256" height="256" alt="image" src="https://github.com/user-attachments/assets/5a5ab2df-7e8d-41cc-83d8-1ab7ade6aef5" />



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
[![PyPI Downloads](https://static.pepy.tech/personalized-badge/flash-mineru?period=total&units=ABBREVIATION&left_color=GREY&right_color=GREEN&left_text=downloads)](https://pepy.tech/projects/flash-mineru)
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

## 🎯 How pipeline parallelism helps

MinerU’s PDF→Markdown path is a **multi-stage pipeline** (e.g. page rendering → VLM → Markdown). If every batch must **finish all stages** before the next batch starts, workers and GPUs **wait on each other**—that shows up as **idle gaps (“bubbles”)** on a timeline and **under-used accelerators**. **Flash-MinerU** (default `MineruEngine`) **overlaps several logical batches** across those stages: while one batch sits in VLM, another can be rendering or writing Markdown, so **compute stays busier end-to-end** without changing MinerU’s operators.

<table width="100%">
<tr>
<td width="50%" valign="top" align="center">
<strong>Left — bubble schedule (before)</strong><br/>
<em>Per-batch serialization; visible GPU idle gaps.</em><br/><br/>
<img src="./docs/bubble.png" alt="Timeline: pipeline bubbles, GPU not fully utilized" width="100%" />
</td>
<td width="50%" valign="top" align="center">
<strong>Right — pipelined (Flash-MinerU)</strong><br/>
<em>Overlapped batches; GPUs keep working.</em><br/><br/>
<img src="./docs/pipelined.png" alt="Timeline: pipeline parallelism, better GPU utilization" width="100%" />
</td>
</tr>
</table>

---

## 📦 Installation

### Basic installation (lightweight mode)

Suitable if you have **already installed the inference backend manually** (e.g., vLLM), or are using an image with a prebuilt environment:

```bash
pip install flash-mineru
```

### Install with vLLM backend enabled (optional)

If you want Flash-MinerU to install vLLM as the inference backend for you:

```bash
pip install flash-mineru[vllm]
```

---

## 🚀 Quickstart

### Minimal Python API example

```python
from flash_mineru import MineruEngine

# Path to PDFs
pdfs = [
    "resnet.pdf",
    "yolo.pdf",
    "text2sql.pdf",
]

engine = MineruEngine(
    model="<path_to_local>/MinerU2.5-2509-1.2B",
    # Model can be downloaded from https://huggingface.co/opendatalab/MinerU2.5-2509-1.2B
    batch_size=2,              # PDFs per logical batch
    replicas=3,                # Parallel vLLM / model instances
    num_gpus_per_replica=0.5, # Fraction of GPU memory per instance (vLLM KV cache)
    save_dir="outputs_mineru", # Output directory for parsed results
    inflight=4,                # Pipeline parallelism depth (v1.0.0 default path; try 8 on large hosts)
)

# Legacy v0.0.4 sequential batching (deprecated): from flash_mineru import MineruEngineLegacy

results = engine.run(pdfs)
print(results)  # list[list[str]], dir name of the output files
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

## 📊 Benchmark

**Scripts:** [English](./docs/BENCHMARK.md) · [简体中文](./docs/BENCHMARK.zh.md)

### Results (368 PDFs, ~8× A100 class machine)

| Method | Inference configuration | Total time |
|----|----|----|
| Flash-MinerU **v1.0.0** | **`MineruEngine`**, 8 replicas, `inflight` 8 | **~8.5 min** |
| MinerU (vanilla) | **Eight hand-spawned** `mineru` processes (this repo’s **Benchmark-mineru.py** `parallel` mode, one GPU per process, `vlm-auto-engine`) | ~14 min |
| Flash-MinerU **v0.0.4** | **`MineruEngineLegacy`**, 8 replicas × 1 GPU, batch size 16 | ~23 min |
| MinerU (vanilla) | vLLM, **single GPU** | ~65 min |

Commands: [docs/BENCHMARK.md](./docs/BENCHMARK.md).

### Summary

- **v1.0.0** is about **~1.7×** faster wall time than the **eight-process** baseline (~8.5 min vs ~14 min)
- **v0.0.4** (`MineruEngineLegacy`) is slower than that baseline (~23 min), which highlights what **pipeline parallelism** adds versus “many full stacks in parallel”
- **~65 min single-GPU** is the same-corpus reference baseline

<details>
<summary><strong>Experimental setup (expand)</strong></summary>

- **Dataset:** 23 paper PDFs (≈9–37 pages each) × 16 copies → **368** files; default folder `test/sample_pdfs`
- **Versions:** MinerU **v2.7.5**; Flash-MinerU **v0.0.4** = `MineruEngineLegacy` (sequential stages per batch); **v1.0.0** = `MineruEngine` (pipeline parallelism, default API)
- **Hardware:** single host, **8 × NVIDIA A100**

</details>

> Note: Throughput-focused. Output shape matches MinerU. Upstream does not ship a polished official multi-GPU “one click” path; the eight-process row is our **benchmark script** sharding **eight separate** `mineru` runs.

---

## 🗺️ Roadmap

* [x] Benchmark scripts & docs — [docs/BENCHMARK.md](./docs/BENCHMARK.md)
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

