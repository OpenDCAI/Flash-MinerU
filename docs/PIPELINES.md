# Running every pipeline

This guide is the shortest supported path from a clean environment to a Flash-MinerU result. Flash-MinerU provides RayOrch execution pipelines for MinerU; it is an independent OpenDCAI project and not an official MinerU distribution. MinerU supplies the document parsing models and runtime components, while Flash-MinerU owns the versioned DAGs, cross-document batching, actor placement, compatibility shims, and the configurations documented here.

## 1. Choose one runtime

MinerU 2.5 and MinerU 4 require incompatible major dependency stacks. Create a separate environment for each generation and install only one extra in an inference environment.

```bash
# MinerU 2.5, 2.5 Pro 2604, and 2.5 Pro 2605
conda create -n flash-mineru25 python=3.11 -y
conda activate flash-mineru25
pip install "flash-mineru[mineru25]==1.1.0"

# MinerU 4 Flash/Basic, or Standard/Advanced with an external VLM server
conda create -n flash-mineru4 python=3.11 -y
conda activate flash-mineru4
pip install "flash-mineru[mineru4]==1.1.0"

# MinerU 4 Advanced with one local vLLM model stack per GPU
conda create -n flash-mineru4-local python=3.11 -y
conda activate flash-mineru4-local
pip install "flash-mineru[mineru4-local-vllm]==1.1.0"
```

The local-vLLM extra is the Flash-MinerU-tested CUDA 12 combination. It is maintained by this project and must not be read as an upstream MinerU support statement. Before loading a model, verify the selected environment:

```bash
python - <<'PY'
import flash_mineru
from importlib.metadata import version
from flash_mineru import pipeline_names

print("flash-mineru:", flash_mineru.__version__)
print("rayorch:", version("rayorch"))
print("pipelines:", pipeline_names())
PY
nvidia-smi
```

## 2. Prepare inputs and models

Place one or more PDFs under `inputs/`. All workers must be able to read the PDF paths and model path and write the output path. A shared filesystem is therefore recommended for a multi-node Ray cluster.

```bash
mkdir -p inputs outputs models
```

Model download is explicit and never starts during `import flash_mineru`.

```python
from flash_mineru import prepare_pipeline

# Download one MinerU 2.5 checkpoint. Change the pipeline name to select a Pro model.
model = prepare_pipeline(
    "v2.5",
    download=True,
    cache_dir="models/huggingface",
)
print(model)

# Run this in a MinerU 4 environment to prepare its model root.
model_root = prepare_pipeline(
    "v4-basic",
    model="models/mineru4",
    download=True,
)
print(model_root)
```

For offline use, pass an existing local directory with `download=False`. MinerU 4 Standard/Advanced require both their small-model root and a VLM. The examples below assume an OpenAI-compatible VLM endpoint at `http://127.0.0.1:18000/v1`; start that service using the model server supported by your MinerU deployment before constructing the engine. Local/Shared Advanced instead load vLLM inside each Ray actor and do not accept `vlm_server_url`.

## 3. One runner for all pipelines

The repository includes [`examples/run_pipeline.py`](../examples/run_pipeline.py). The same program runs every bundled pipeline; only the pipeline name, model path, and optional server settings change. The full script is shown here so PyPI users can also copy it directly.

```python
import argparse
from pathlib import Path

from flash_mineru import MineruEngine, pipeline_names


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline", required=True, choices=pipeline_names())
    parser.add_argument("--model", required=True, help="Checkpoint or MinerU 4 model root")
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--gpus", type=int, default=1)
    parser.add_argument("--vlm-server-url", default="")
    parser.add_argument("--vlm-model", default="")
    return parser.parse_args()


def collect_pdfs(path: Path) -> list[str]:
    pdfs = [path] if path.is_file() else sorted(path.glob("*.pdf"))
    if not pdfs:
        raise SystemExit(f"no PDFs found at {path}")
    return [str(pdf.resolve()) for pdf in pdfs]


args = parse_args()

with MineruEngine(
    pipeline_version=args.pipeline,
    model=args.model,
    save_dir=str(args.output),
    replicas=args.gpus,                         # Start with 1; scale to one actor per GPU.
    num_gpus_per_replica=1.0,
    batch_size=16,                              # Groups returned output paths only.
    input_batch_size=24,                        # PDFs admitted per RayOrch input batch.
    inflight=4,                                 # Input batches allowed to overlap.
    vlm_server_url=args.vlm_server_url,
    vlm_model=args.vlm_model,
) as engine:
    batches = engine.run(collect_pdfs(args.input))

for batch in batches:
    for output in batch:
        print(output)
```

Start with one PDF and one GPU:

```bash
python examples/run_pipeline.py \
  --pipeline v2.5 \
  --model /shared/models/MinerU2.5-2509-1.2B \
  --input inputs/example.pdf \
  --output outputs/v2.5 \
  --gpus 1
```

Once this succeeds, point `--input` at a directory and increase `--gpus` to the number of available GPUs. RayOrch keeps the actor models resident for the lifetime of `MineruEngine`, batches ready pages/windows from different PDFs, and restores each result to its original document order.

The engine automatically uses `engine_gpu_util_rate_to_ray_cap=0.1` for the two local Advanced pipelines and `0.9` elsewhere. Override it in `MineruEngine` only after measuring available GPU memory.

## 4. Commands for every bundled pipeline

### MinerU 2.5 family

Use the `flash-mineru25` environment. Each pipeline has its own checkpoint and otherwise uses the same command.

```bash
python examples/run_pipeline.py --pipeline v2.5          --model /shared/models/MinerU2.5-2509-1.2B      --input inputs --output outputs/v2.5          --gpus 4
python examples/run_pipeline.py --pipeline v2.5-pro-2604 --model /shared/models/MinerU2.5-Pro-2604-1.2B --input inputs --output outputs/v2.5-pro-2604 --gpus 4
python examples/run_pipeline.py --pipeline v2.5-pro-2605 --model /shared/models/MinerU2.5-Pro-2605-1.2B --input inputs --output outputs/v2.5-pro-2605 --gpus 4
```

### MinerU 4 Flash and Basic

Use the `flash-mineru4` environment. These pipelines use the local MinerU 4 small-model root and require no VLM endpoint.

```bash
python examples/run_pipeline.py --pipeline v4-flash --model /shared/models/mineru4 --input inputs --output outputs/v4-flash --gpus 4
python examples/run_pipeline.py --pipeline v4-basic --model /shared/models/mineru4 --input inputs --output outputs/v4-basic --gpus 4
```

`v4-flash` is supported for completeness, but the validated workload contains little expensive batchable work and was slower than native MinerU. Prefer it for compatibility experiments rather than as the default throughput demonstration.

### MinerU 4 Standard and Advanced with an external VLM

Use the `flash-mineru4` environment and start an OpenAI-compatible MinerU VLM service separately. The VLM service consumes its own GPU resources; `--gpus` controls only Flash-MinerU actor replicas.

For the MinerU 2.5 VLM used by the validated Standard/Advanced runs, one supported vLLM command is:

```bash
CUDA_VISIBLE_DEVICES=0 vllm serve opendatalab/MinerU2.5-2509-1.2B \
  --host 127.0.0.1 \
  --port 18000 \
  --logits-processors mineru_vl_utils:MinerULogitsProcessor
```

Run the Flash-MinerU actors on the remaining GPUs, for example with `CUDA_VISIBLE_DEVICES=1,2,3` and `--gpus 3`. If you use another server implementation, it must expose the OpenAI-compatible endpoint expected by MinerU.

```bash
CUDA_VISIBLE_DEVICES=1,2,3 python examples/run_pipeline.py --pipeline v4-standard --model /shared/models/mineru4 --input inputs --output outputs/v4-standard --gpus 3 --vlm-server-url http://127.0.0.1:18000/v1
CUDA_VISIBLE_DEVICES=1,2,3 python examples/run_pipeline.py --pipeline v4-advanced --model /shared/models/mineru4 --input inputs --output outputs/v4-advanced --gpus 3 --vlm-server-url http://127.0.0.1:18000/v1
```

If the endpoint exposes several models, also pass `--vlm-model MODEL_NAME`. Test the endpoint independently before running Flash-MinerU.

### MinerU 4 Advanced with local vLLM

Use the `flash-mineru4-local` environment. These pipelines load one complete model stack in every GPU actor, so begin with `--gpus 1`. `v4-advanced-shared` is the recommended local topology: separate Infer and Finish DAG stages reuse the same resident actor pool.

```bash
python examples/run_pipeline.py --pipeline v4-advanced-local  --model /shared/models/mineru4 --input inputs --output outputs/v4-advanced-local  --gpus 4
python examples/run_pipeline.py --pipeline v4-advanced-shared --model /shared/models/mineru4 --input inputs --output outputs/v4-advanced-shared --gpus 4
```

The engine automatically selects the tested local-vLLM starting value `engine_gpu_util_rate_to_ray_cap=0.1`. Increase it only after confirming free memory on the target GPU and leave room for Layout, MFR, OCR, Ray, and CUDA allocations.

## 5. Ray clusters and separate environments

Connect the driver to Ray before constructing the engine and use `runtime_env` when actors must run in a named conda environment:

```python
import ray
from flash_mineru import MineruEngine

ray.init(address="auto")

with MineruEngine(
    pipeline_version="v4-basic",
    model="/shared/models/mineru4",
    save_dir="/shared/outputs/v4-basic",
    batch_size=16,
    replicas=4,
    runtime_env={"conda": "flash-mineru4"},
) as engine:
    results = engine.run(["/shared/inputs/example.pdf"])
```

The named environment must already exist on each worker. Flash-MinerU does not copy multi-gigabyte model directories through Ray's working directory; use shared storage or the same mounted path on every node.

## 6. Output and first-run checks

MinerU 2.5 writes `<output>/<pdf_name>/vlm/<pdf_name>.md`. MinerU 4 writes `<output>/<pdf_name>/<flash|basic|standard|advanced>/markdown.md`. On the first run, check:

1. Every input PDF has one output directory.
2. The expected Markdown file exists and is non-empty.
3. GPU actors remain alive while multiple PDFs are processed.
4. For a cluster, every node resolves the same input, model, and output paths.

## 7. Common failures

| Symptom | Check |
|---|---|
| MinerU import/version error | Activate the matching environment; do not combine `mineru25` and `mineru4` extras. |
| Model not found | Pass an existing directory or call `prepare_pipeline(..., download=True)` in the matching runtime. |
| Ray reports unavailable GPUs | Reduce `--gpus`, check `nvidia-smi`, and ensure the Ray cluster advertises GPU resources. |
| Local Advanced actor exits/OOMs | Start with one replica and utilization `0.1`; confirm the full model stack fits before scaling. |
| Standard/Advanced HTTP failure | Verify the VLM endpoint and model name independently; include `/v1` when required by the server. |
| Worker cannot read a file | Use absolute paths visible at the same location on every worker. |

Performance methodology and the complete 368-PDF results are documented in [BENCHMARK.md](./BENCHMARK.md).
