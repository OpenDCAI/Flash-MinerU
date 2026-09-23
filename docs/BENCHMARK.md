# Benchmarking Flash-MinerU

[简体中文](./BENCHMARK.zh.md) | English

`test/Benchmark-versioned.py` is the reproducible benchmark entry point for both native MinerU and Flash-MinerU. It uses the same ordered PDF corpus, model, MinerU environment, parse mode, image-analysis setting, and GPU count; records a corpus SHA-256, package and GPU versions, wall time, throughput, GPU samples, and output validation; and rejects comparisons whose key configurations differ.

## Pipeline migration validation

On September 21, 2026, every pipeline exposed by MinerU 3.4.4 and the current MinerU 4.0.5 source snapshot (`94828a88d80ab8a72c21956de0604bf14f4cc9fc`) was exercised through both native MinerU and the corresponding named Flash-MinerU/RayOrch pipeline on the same real 12-page PDF. Every run produced a complete document with 12/12 pages, and every native-versus-Flash comparison preserved page and block counts.

| Flash-MinerU pipeline | Scheduling granularity | Native-equivalence result |
|---|---|---|
| `v2.5` | Page | 1/1 valid; token Jaccard 0.9934, token F1 0.9984 |
| `v2.5-pro-2604` | Page | 1/1 valid; token Jaccard 0.9913, token F1 0.9986 |
| `v2.5-pro-2605` | Page | 1/1 valid; token Jaccard 0.9970, token F1 0.9987 |
| `v4-flash` | Whole document for TXT; windows for OCR | 1/1 valid; Markdown byte-identical |
| `v4-basic` | Ordered windows | 1/1 valid; Markdown byte-identical |
| `v4-standard` | Ordered windows plus layout-guided VLM | 1/1 valid; Markdown byte-identical |
| `v4-advanced` | Ordered windows plus full-page VLM | 1/1 valid; page/block structure matched, token Jaccard 0.9988 and token F1 0.9996 |

This matrix proves graph construction, real model execution, output generation, and native-output equivalence. Its one-document timings are deliberately **not** presented as throughput results. The machine-readable evidence is stored in [`docs/benchmark-results/all-pipelines-e2e-2026-09-21.json`](./benchmark-results/all-pipelines-e2e-2026-09-21.json). The 368-PDF experiment below remains the performance result.

The additional `v4-advanced-local` and `v4-advanced-shared` deployment variants were validated on September 22, 2026 with the full 368-PDF corpus. Both replace the shared HTTP server with one complete resident model stack per GPU; `v4-advanced-shared` additionally splits inference and finishing into separate logical Calls that reuse the same physical RayOrch Actor Pool.

## Full 368-PDF results

Across September 20–22, 2026, the registered pipelines were measured on the same **368 PDFs / 7,072 pages** corpus. The three MinerU 2.5 pipelines, V4 Flash, V4 Basic, V4 Advanced Local, and V4 Advanced Shared used **4× NVIDIA H20** directly. V4 Standard and the original V4 Advanced used **three runner GPUs plus one shared HTTP VLM-server GPU** on both sides; server startup is excluded from both native and Flash timings. Flash execution time starts after Ray actors and models are ready, while Flash cold time also includes Ray startup, actor/model initialization, and cleanup.

| Pipeline | Native | Flash execution | Flash cold | Execution speedup | Cold speedup | Jaccard / F1 |
|---|---:|---:|---:|---:|---:|---:|
| `v2.5` | 1,177.01 s | 439.72 s | 487.25 s | **2.68×** | **2.42×** | 0.9902 / 0.9931 |
| `v2.5-pro-2604` | 1,449.57 s | 545.72 s | 598.88 s | **2.66×** | **2.42×** | 0.9898 / 0.9869 |
| `v2.5-pro-2605` | 1,468.23 s | 553.58 s | 605.43 s | **2.65×** | **2.43×** | 0.9903 / 0.9883 |
| `v4-flash` | 1,044.69 s | 1,157.28 s | 1,165.29 s | 0.90× | 0.90× | 0.9997 / 0.9999 |
| `v4-basic` | 1,642.11 s | 1,550.47 s | 1,559.31 s | **1.06×** | **1.05×** | 0.9999 / 1.0000 |
| `v4-standard` | 1,822.74 s | 1,435.38 s | 1,443.93 s | **1.27×** | **1.26×** | 1.0000 / 0.9999 |
| `v4-advanced` | 2,559.79 s | 1,899.27 s | 1,907.68 s | **1.35×** | **1.34×** | 0.9934 / 0.9906 |
| `v4-advanced-local` | 1,942.44 s | 1,350.25 s | 1,363.53 s | **1.44×** | **1.42×** | 0.9940 / 0.9911 |
| `v4-advanced-shared` | 1,942.44 s | 1,234.60 s | 1,248.52 s | **1.57×** | **1.56×** | 0.9945 / 0.9906 |

Every row completed **368/368 PDFs and 7,072/7,072 pages**, with no page-count mismatch. V4 Flash is an important negative result: most TXT inputs already take MinerU's inexpensive whole-document native path, so a fine-grained DAG adds scheduling overhead without exposing an expensive batchable stage. V4 Basic originally had the same issue at its model boundary: the Analyze UDF received batches of windows but finished each window independently. The enhanced implementation keeps the public `split -> analyze -> assemble` graph unchanged, flattens ready pages inside one Analyze actor, batches MinerU's formula/table/OCR stages across windows, and then restores ownership and order. This reduced its Flash execution time from **1,736.74 s to 1,550.47 s** (a **10.7% reduction**) and changed the native-over-Flash result from **0.95× to 1.06×** without changing page or block counts.

`v4-advanced-local` answers a different deployment question from `v4-advanced`. Both Native and Flash use four complete local stacks, one per H20, and each stack keeps Layout, Two-Step vLLM, MFR, and OCR resident together with `gpu_memory_utilization=0.1`. Flash keeps the Two-Step model call inside one Analyze UDF, but schedules eight-page windows independently and overlaps CPU rendering with four GPU actors. The result is **1.44× execution speedup** and **1.42× cold end-to-end speedup**, saving 578.91 seconds. Mean token Jaccard is **0.9940**, mean token multiset F1 is **0.9911**, all page counts match, and 55 of 368 documents have a block-count difference; these differences are concentrated in nondeterministic VLM outputs rather than missing documents or pages.

`v4-advanced-shared` tests RayOrch's shared-actor abstraction directly. The graph exposes `Infer(Layout + Two-Step VLM)` and `Finish(MFR + OCR + fill)` as independent logical Calls with separate lineage and ready queues, but both Calls reuse the same four physical actors and resident model stacks. This lowers execution time from the monolithic local pipeline's **1,350.25 s to 1,234.60 s**, a **9.4% throughput improvement**, and raises the equivalent native comparison to **1.57× execution speedup** and **1.56× cold end-to-end speedup**. All 368 documents and 7,072 pages completed; versus Native, mean token Jaccard is **0.9945**, mean token multiset F1 is **0.9906**, and the 54 block-count differences are attributable to nondeterministic VLM decoding rather than lineage loss.

The shared-Call batch-size ablation also shows why larger is not automatically better. On the 48-PDF tuning subset, increasing the batch from 16 to 32 reduced execution from **244.57 s to 226.34 s**, but batch 64 regressed to **363.84 s**. More importantly, the full 368-PDF run with batch 32 took **1,372.62 s**, which is **11.2% slower** than batch 16. Peak memory remained near 15.5–16.0 GiB, so the limiting factor is not vLLM KV-cache capacity: oversized Finish calls create larger MFR/OCR work quanta and longer shared-pool head-of-line blocking. The production default therefore remains **16**.

The complete machine-readable result, including configurations and the V4 Basic before/after record, is stored in [`docs/benchmark-results/all-pipelines-368-2026-09-22.json`](./benchmark-results/all-pipelines-368-2026-09-22.json). The local-stack and shared-actor runs, including GPU, stage, quality, and batch-ablation details, are stored separately in [`v4-advanced-local-4xh20-368-2026-09-22.json`](./benchmark-results/v4-advanced-local-4xh20-368-2026-09-22.json) and [`v4-advanced-shared-4xh20-368-2026-09-22.json`](./benchmark-results/v4-advanced-shared-4xh20-368-2026-09-22.json).

## Detailed MinerU 2.5 result

The following result was measured on September 20, 2026 with **368 PDFs / 7,072 pages** on one host with **4× NVIDIA H20**. Both sides used MinerU 3.4.4, `mineru-vl-utils` 1.2.1, vLLM 0.10.1.1, `parse_mode=auto`, `image_analysis=false`, and the same local MinerU 2.5 model. The native baseline used four independent one-GPU MinerU workers and deterministically balanced them to exactly 1,768 pages per GPU. Flash-MinerU used four RayOrch-managed VLM replicas.

| Runner | Measured time | PDF/s | page/s |
|---|---:|---:|---:|
| Native MinerU, end to end | 1,177.01 s | 0.313 | 6.008 |
| Flash-MinerU, execution after actor initialization | 439.72 s | 0.837 | 16.083 |
| Flash-MinerU, cold end to end | 487.25 s | 0.755 | 14.514 |

- **Steady execution speedup:** `1177.01 / 439.72 = 2.68×`
- **Cold end-to-end speedup:** `1177.01 / 487.25 = 2.42×`
- Both runners completed **368/368 PDFs and 7,072/7,072 pages**.
- Markdown comparison: mean token Jaccard **0.9902**, mean token multiset F1 **0.9931**, and no page-count mismatch.
- VLM decoding is not bitwise deterministic across batching layouts. The lowest per-file score came from a difficult long-table document, and repeated copies also varied within native MinerU itself; therefore the report records semantic similarity and structural counts instead of requiring byte-identical Markdown.

The machine-readable summary is stored in [`docs/benchmark-results/v2.5-4xh20-368-2026-09-20.json`](./benchmark-results/v2.5-4xh20-368-2026-09-20.json).

## Run the same comparison

Run all commands from the repository root. Use an environment containing the selected MinerU version, Flash-MinerU dependencies, and the released `rayorch>=0.1.1,<0.2`.

```bash
export MODEL=/path/to/MinerU2.5-2509-1.2B
export PDF_DIR=/path/to/368-pdfs
export PYTHONPATH=/path/to/rayorch/site-packages:.
```

### 1. Native MinerU

```bash
python -u test/Benchmark-versioned.py native \
  --pipeline v2.5 \
  --pdf-dir "$PDF_DIR" \
  --output-dir /tmp/mineru-native \
  --report /tmp/mineru-native.json \
  --limit 368 \
  --gpus 4 \
  --parse-mode auto \
  --no-image-analysis \
  --model "$MODEL" \
  --worker-batch-size 16
```

The script runs one native MinerU process per GPU. PDFs are assigned with deterministic page-count balancing so the baseline is not weakened by a long-document tail.

### 2. Flash-MinerU

```bash
python -u test/Benchmark-versioned.py flash \
  --pipeline v2.5 \
  --pdf-dir "$PDF_DIR" \
  --output-dir /tmp/flash-mineru \
  --report /tmp/flash-mineru.json \
  --limit 368 \
  --gpus 4 \
  --parse-mode auto \
  --no-image-analysis \
  --model "$MODEL" \
  --batch-size 16 \
  --ocr-batch-size 128 \
  --input-batch-size 24 \
  --inflight 4
```

`startup_seconds` covers Ray startup, actor creation, model loading, and actor readiness. `execution_seconds` covers `engine.run(...)`. `wall_seconds` covers construction, execution, and cleanup.

### 3. Validate comparability and outputs

```bash
python -u test/Benchmark-versioned.py compare \
  --native-report /tmp/mineru-native.json \
  --flash-report /tmp/flash-mineru.json \
  --report /tmp/mineru-comparison.json
```

The comparison checks corpus SHA-256 and the relevant runtime configuration before calculating both execution-only and cold end-to-end speedups. It then validates every output, compares page and block counts, and reports Markdown token similarity.

## Short validation ladder

Before a full corpus run, use `--limit 1`, then `--limit 4`, `--limit 12`, and `--limit 48`. This catches dependency, model, GPU cleanup, output-layout, and large-batch quality regressions without paying the full benchmark cost each time.

## Other versioned pipelines

The same runner accepts `v2.5-pro-2604`, `v2.5-pro-2605`, `v4-flash`, `v4-basic`, `v4-standard`, `v4-advanced`, `v4-advanced-local`, and `v4-advanced-shared`. Native and Flash commands must use the same pipeline-specific model files and backend configuration. V4 Standard and the original V4 Advanced require an equivalent VLM server topology on both sides before their numbers are comparable. V4 Advanced Local instead requires a local vLLM runtime and one complete stack per GPU; the published run uses `--gpu-memory-utilization 0.1 --window-size 8 --layout-batch-size 8 --ocr-batch-size 8 --input-batch-size 16 --inflight 3`. The comparison command rejects failed reports, incomplete output validation, zero wall time, and zero Flash execution time instead of producing a misleading speedup.

For V4 Basic, `--ocr-batch-size` controls the number of ready windows delivered to each Analyze actor call, not MinerU's internal OCR detector batch size. The published result uses `--window-size 8 --ocr-batch-size 32`: eight pages retains useful scheduling granularity, while up to 32 ready windows can contribute pages to one local-model invocation. Increasing the window to 16 pages was slower on the 48-PDF tuning subset, so the published 368-PDF run retains eight-page windows.

To reproduce the four-local-stack Advanced comparison, run the same `native`, `flash`, and `compare` sequence above with `--pipeline v4-advanced-local --image-analysis --small-backend torch --vlm-engine vllm --gpu-memory-utilization 0.1`. For the Flash run, additionally use `--batch-size 16 --input-batch-size 16 --inflight 3 --ocr-batch-size 8 --window-size 8 --layout-batch-size 8`. Both sides must run on four GPUs and use the same MinerU 4 model root.

To reproduce the shared-actor variant, replace the Flash pipeline with `v4-advanced-shared` and use `--ocr-batch-size 16`; the matching Native baseline is the same four-local-stack Advanced workload. Here `--ocr-batch-size` is the maximum number of ready windows in either shared model Call, not only an OCR-library batch. Keep the published value of 16 for the full corpus: although 32 improved the 48-PDF subset, it regressed on all 368 PDFs, and 64 caused severe head-of-line blocking.

The older `Benchmark-mineru.py`, `Benchmark-flashmineru.py`, and `Benchmark-flashmineru_dag.py` scripts are retained for historical workflows, but new published results should use `Benchmark-versioned.py`.

## Runtime installation and isolation

Use `pip install "flash-mineru[mineru25]"` for the three MinerU 2.5 pipelines, `pip install "flash-mineru[mineru4]"` for MinerU 4 with an external HTTP VLM server, or `pip install "flash-mineru[mineru4-local-vllm]"` for a local MinerU 4 vLLM backend. MinerU 2.5 and MinerU 4 intentionally live in separate environments because their `mineru-vl-utils` and Transformers major-version requirements conflict. The local-vLLM extra deliberately selects the Flash-MinerU-tested CUDA-12 path (`MinerU 4.0.x` with `vLLM 0.10.1.1`) rather than MinerU's `full` extra, which may currently resolve to a CUDA-13 vLLM wheel. This compatibility path is maintained by Flash-MinerU and should not be read as an upstream MinerU support statement.

For a driver that does not contain MinerU 4 itself, place the prepared model root on storage visible to every worker and pass `runtime_env={"conda": "mineru4"}` to `MineruEngine`. With the default `prepare=False`, the driver resolves the existing shared path while MinerU runtime validation occurs when the Ray actors initialize. Model downloads remain explicit and should be run in the matching actor environment before the benchmark.
