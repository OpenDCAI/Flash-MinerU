# Flash-MinerU ⚡️📄

<div align="center">
<img width="220" height="220" alt="Flash-MinerU" src="https://github.com/user-attachments/assets/5a5ab2df-7e8d-41cc-83d8-1ab7ade6aef5" />

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

[简体中文](./README-zh.md) | English · [Benchmark](./docs/BENCHMARK.md) · [Results](./docs/benchmark-results/)
</div>

> **Scale PDF understanding without flattening document lineage.**

Flash-MinerU is a versioned, RayOrch-powered execution layer for [MinerU](https://github.com/opendatalab/MinerU). It turns each PDF into page or window work (`1 → M`), batches ready work from different documents on shared CPU/GPU actors, and reduces the results back to the correct document and order (`M → 1`).

It keeps MinerU's models and output conventions. Each supported MinerU runtime/version is isolated in an explicit pipeline, so applications can upgrade one path at a time while keeping one small Python API.

> [!IMPORTANT]
> Flash-MinerU is an independent OpenDCAI project, not an official MinerU distribution. It reuses and adapts MinerU runtime components under the repository license, while its RayOrch DAGs, multi-GPU scheduling, release cadence, tested dependency combinations, and performance claims are maintained by Flash-MinerU. Please report Flash-MinerU pipeline issues here; report upstream model/runtime issues to MinerU when they also reproduce with official MinerU.

```mermaid
flowchart LR
    A[PDFs] --> B[Ordered pages / windows]
    B --> C[CPU render]
    C --> D[Shared GPU actor pool<br/>cross-document batching]
    D --> E[Restore lineage and order]
    E --> F[Markdown / JSON]
```

## How the layers fit together

The three names describe different responsibilities:

| Layer | Responsibility in this project |
|---|---|
| [**MinerU**](https://github.com/opendatalab/MinerU) | Parses a page or window and produces the document artifacts and output conventions. |
| [**Ray**](https://github.com/ray-project/ray) | Supplies the physical distributed runtime: actors, GPU placement, resources, RPCs, and object storage. |
| [**RayOrch**](https://github.com/OpenDCAI/RayOrch) | Supplies the logical dataflow: explicit fan-out/fan-in, lineage, readiness, cross-document batching, and ordered reconstruction. |
| **Flash-MinerU** | Connects each supported MinerU version to a versioned RayOrch pipeline and exposes one `MineruEngine` API. |

Ray can run the actors, but the application still needs a rule for which page belongs to which PDF, when a parent is complete, and how out-of-order results are rebuilt. RayOrch keeps those rules outside the model code; Flash-MinerU fills the stages with MinerU operations.

### What `1 → M → 1` means here

For one input PDF, the pipeline has three logical steps:

1. **`1 → M` — expand:** render one PDF into a variable number of pages or windows. Each child keeps its parent document and ordinal.
2. **Batch ready children:** pages or windows from different PDFs can share one CPU/GPU actor batch. The physical batch is temporary; it does not change ownership or order.
3. **`M → 1` — reduce:** collect the completed children by parent and ordinal, then write one ordered document result. A document can finish as soon as its own children are ready.

This is why the system can improve accelerator utilization without flattening the document contract: the physical schedule is shared, while the logical lineage remains per document.

## Why the execution layer matters

A model-hosted document pipeline is usually limited by scheduling rather than a single model call: documents have different lengths, CPU and GPU stages progress at different rates, and batching must not lose document ownership or page order. Flash-MinerU uses RayOrch to overlap those stages, batch ready work across documents, and keep lineage explicit throughout the DAG.

The `v4-advanced-shared` pipeline also demonstrates shared actor reuse: `Infer` and `Finish` are separate logical DAG calls but run on the same physical four-replica model pool. Each actor loads one complete model stack, while intermediate state travels through RayOrch ports instead of relying on replica affinity. On the validated 368-PDF workload this reduced execution time from 1,350.25 s for `v4-advanced-local` to 1,234.60 s, a further **9.4% improvement**.

## Results

The following results use the same 368 PDFs (7,072 pages) on 4× NVIDIA H20. “Execution” measures `Executor.run` after actor initialization; “cold” includes Ray startup, model initialization, execution, and cleanup. Output quality is compared with the corresponding native MinerU run.

| Pipeline | Native | Flash execution | Execution speedup | Cold speedup | Mean Jaccard / F1 |
|---|---:|---:|---:|---:|---:|
| `v2.5` | 1,177.01 s | 439.72 s | **2.68×** | 2.42× | 0.9902 / 0.9931 |
| `v2.5-pro-2604` | 1,449.57 s | 545.72 s | **2.66×** | 2.42× | 0.9898 / 0.9869 |
| `v2.5-pro-2605` | 1,468.23 s | 553.58 s | **2.65×** | 2.43× | 0.9903 / 0.9883 |
| `v4-flash` | 1,044.69 s | 1,157.28 s | 0.90× | 0.90× | 0.9997 / 0.9999 |
| `v4-basic` | 1,642.11 s | 1,550.47 s | 1.06× | 1.05× | 0.9999 / 1.0000 |
| `v4-standard` | 1,822.74 s | 1,435.38 s | **1.27×** | 1.26× | 1.0000 / 0.9999 |
| `v4-advanced` | 2,559.79 s | 1,899.27 s | **1.35×** | 1.34× | 0.9934 / 0.9906 |
| `v4-advanced-local` | 1,942.44 s | 1,350.25 s | **1.44×** | 1.42× | 0.9940 / 0.9911 |
| `v4-advanced-shared` | 1,942.44 s | **1,234.60 s** | **1.57×** | **1.56×** | 0.9945 / 0.9906 |

All runs completed 368/368 documents and 7,072/7,072 pages without page-count mismatches. `v4-flash` is included as a transparent negative result: its workload has little expensive batchable model work, so a finer-grained DAG adds overhead instead of improving throughput. See the [benchmark methodology](./docs/BENCHMARK.md) and [machine-readable results](./docs/benchmark-results/) for configurations, timing policy, quality checks, and batch ablations.

These numbers describe the published 1.1.0 benchmark configuration, not a universal speed guarantee. Choose a pipeline for its model/runtime compatibility first, then tune replicas and batching on your own workload.

## Choose a pipeline

There are four practical pipeline families. The exact names are versioned because MinerU 2.5 and MinerU 4 do not share a dependency stack.

| Family | Pipeline names | Install extra | Execution shape |
|---|---|---|---|
| MinerU 2.5 | `v2.5`, `v2.5-pro-2604`, `v2.5-pro-2605` | `mineru25` | Cross-document page batching on a shared VLM pool |
| MinerU 4 local stages | `v4-flash`, `v4-basic` | `mineru4` | Window scheduling for local small-model stages; no VLM endpoint |
| MinerU 4 with a VLM service | `v4-standard`, `v4-advanced` | `mineru4` | Local small models plus a shared HTTP or local VLM |
| MinerU 4 local vLLM | `v4-advanced-local`, `v4-advanced-shared` | `mineru4-local-vllm` | Local model stacks; `shared` reuses one actor pool across logical calls |

For model preparation, server requirements, and a runnable command for every name, use the [pipeline guide](./docs/PIPELINES.md).

## Installation

MinerU 2.5 and MinerU 4 use incompatible major dependency stacks. Install the base package on a lightweight driver, or select exactly one runtime extra for an inference environment.

| Installation | Use case |
|---|---|
| `pip install flash-mineru` | Public API, RayOrch, and a lightweight driver |
| `pip install "flash-mineru[mineru25]"` | MinerU 2.5 and 2.5 Pro pipelines |
| `pip install "flash-mineru[mineru4]"` | MinerU 4 small models with an external HTTP VLM server |
| `pip install "flash-mineru[mineru4-local-vllm]"` | MinerU 4 with local vLLM, including `v4-advanced-local` and `v4-advanced-shared` |

Do not install the MinerU 2.5 and MinerU 4 extras in the same environment. Use separate conda environments and RayOrch `runtime_env` when one driver must run both generations. `flash-mineru[vllm]` remains an old MinerU 2.5 compatibility alias and is not recommended for new setups.

## Prepare models

Model preparation is explicit: importing Flash-MinerU never downloads checkpoints.

### Use an existing local checkpoint

```python
from flash_mineru import prepare_pipeline

model = prepare_pipeline(
    "v2.5",
    model="/shared/models/MinerU2.5-2509-1.2B",
)
```

### Download a MinerU 2.5 checkpoint

```python
model = prepare_pipeline(
    "v2.5-pro-2605",             # Uses the registered default Hugging Face repo.
    download=True,                # Network access is opt-in.
    cache_dir="/shared/hf-cache",
)
```

Registered defaults are `opendatalab/MinerU2.5-2509-1.2B`, `opendatalab/MinerU2.5-Pro-2604-1.2B`, and `opendatalab/MinerU2.5-Pro-2605-1.2B`.

### Prepare a MinerU 4 model root

MinerU 4 uses a model root rather than one checkpoint directory. Prepare it inside the MinerU 4 environment and place it on storage visible to every worker:

```python
model_root = prepare_pipeline(
    "v4-advanced-shared",
    model="/shared/models/mineru4",
    download=True,
)
```

With `download=False` (the default), an existing shared root can be passed by a lightweight driver and the authoritative runtime/model validation occurs when the actors start. `v4-standard` and `v4-advanced` only need local small models when an external `vlm_server_url` is configured; `v4-advanced-local` and `v4-advanced-shared` require a local vLLM model and do not accept an HTTP VLM server.

## Run

### Recommended Python API

```python
from flash_mineru import MineruEngine, prepare_pipeline

pdfs = ["paper-a.pdf", "paper-b.pdf"]
model = prepare_pipeline("v2.5", model="/shared/models/MinerU2.5-2509-1.2B")

with MineruEngine(
    pipeline_version="v2.5",
    model=model,
    save_dir="outputs",
    replicas=4,             # Usually one model actor per GPU.
    batch_size=16,          # Compatibility grouping of returned output paths.
    ocr_batch_size=128,     # Ready pages batched into one model call.
    input_batch_size=24,    # PDFs admitted to one RayOrch input batch.
    inflight=4,             # Input batches allowed to overlap.
) as engine:
    results = engine.run(pdfs)

print(results)
```

The context manager releases persistent Ray actors after the run. If an engine must stay alive across multiple calls, construct it once and call `engine.close()` when finished.

For the shortest path from installation to a working run, and copy-paste configurations for every bundled pipeline, see **[Running every pipeline](./docs/PIPELINES.md)**. Start with one PDF and `replicas=1`, verify the output, and then increase replicas to the number of available GPUs.

### MinerU 4 Advanced with a shared actor pool

```python
from flash_mineru import MineruEngine, prepare_pipeline

model_root = prepare_pipeline(
    "v4-advanced-shared",
    model="/shared/models/mineru4",
)

with MineruEngine(
    pipeline_version="v4-advanced-shared",
    model=model_root,
    save_dir="outputs-v4",
    replicas=4,
    batch_size=16,
    ocr_batch_size=16,                  # Validated default for the shared pool.
    input_batch_size=16,
    inflight=3,
    num_gpus_per_replica=1.0,           # Ray reserves one GPU for each actor.
    engine_gpu_util_rate_to_ray_cap=0.1,# Local vLLM memory-utilization setting.
    image_analysis=True,
) as engine:
    results = engine.run(pdfs)
```

### Existing Ray cluster or separate actor environment

```python
import ray
from flash_mineru import MineruEngine

ray.init(address="auto")

with MineruEngine(
    pipeline_version="v4-standard",
    model="/shared/models/mineru4",
    save_dir="outputs-v4",
    replicas=3,
    batch_size=16,
    runtime_env={"conda": "mineru4"},
    vlm_server_url="http://vlm-server:8000/v1",
) as engine:
    results = engine.run(pdfs)
```

The named environment must exist on the worker nodes and contain Flash-MinerU plus the selected MinerU runtime. PDF paths, model roots, and output paths must be visible from those workers.

## Pipeline catalog

```python
from flash_mineru import pipeline_names

print(pipeline_names())
```

| Pipeline | Runtime | Execution shape |
|---|---|---|
| `v2.5` | `mineru25` | Cross-document page batching on a shared VLM pool |
| `v2.5-pro-2604`, `v2.5-pro-2605` | `mineru25` | Page batching followed by ordered document-level table merging |
| `v4-flash`, `v4-basic` | `mineru4` | Window scheduling for the local small-model stages |
| `v4-standard`, `v4-advanced` | `mineru4` | Local small models plus a shared HTTP or local VLM |
| `v4-advanced-local` | `mineru4-local-vllm` | One complete local model stack per GPU actor |
| `v4-advanced-shared` | `mineru4-local-vllm` | Separate Infer/Finish calls sharing the same resident actor pool |

Each implementation is self-contained under `flash_mineru/pipelines/<version>/` with its pipeline, UDFs, and preparation logic. This keeps upstream MinerU version changes isolated and makes new pipelines easier to review.

The table describes the required installation extra, not an interchangeability promise between MinerU generations. Use [the pipeline guide](./docs/PIPELINES.md) for model preparation, external-server requirements, runnable examples, recommended starting values, and troubleshooting.

## Outputs

MinerU 2.5 writes each result under:

```text
<save_dir>/<pdf_name>/vlm/<pdf_name>.md
```

MinerU 4 preserves its tier-specific layout:

```text
<save_dir>/<pdf_name>/<flash|basic|standard|advanced>/markdown.md
```

`MineruEngine.run()` preserves the historical `list[list[str]]` return shape. `MineruEngineLegacy` remains available for compatibility with the deprecated sequential implementation, but new integrations should use `MineruEngine`.

## Acknowledgements

Flash-MinerU builds on [MinerU](https://github.com/opendatalab/MinerU), [Ray](https://github.com/ray-project/ray), [RayOrch](https://github.com/OpenDCAI/RayOrch), and [vLLM](https://github.com/vllm-project/vllm). We thank their authors and contributors.

## License

Flash-MinerU is based on and contains modified source code from MinerU. This repository is licensed under the [MinerU Open Source License](./LICENSE), which is Apache License 2.0 with additional terms. Please review its commercial-use thresholds and attribution requirements before deployment. Third-party dependencies remain under their respective licenses; the complete repository license text is in [`LICENSE`](./LICENSE).
