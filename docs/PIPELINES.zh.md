# 运行全部管线

这份指南给出从干净环境到 Flash-MinerU 结果的最短受支持路径。Flash-MinerU 是基于 MinerU 构建的 RayOrch 执行管线，是 OpenDCAI 独立维护的项目，并非 MinerU 官方发行版。MinerU 提供文档解析模型和运行时组件；Flash-MinerU 负责版本化 DAG、跨文档 batch、Actor 放置、兼容补丁以及本文记录的配置。

## 1. 选择一套运行环境

MinerU 2.5 与 MinerU 4 依赖不同的大版本运行栈。请分别创建环境，并在一个推理环境中只安装一组 extra。

```bash
# MinerU 2.5、2.5 Pro 2604 与 2.5 Pro 2605
conda create -n flash-mineru25 python=3.11 -y
conda activate flash-mineru25
pip install "flash-mineru[mineru25]==1.1.0"

# MinerU 4 Flash/Basic，或者搭配外部 VLM Server 的 Standard/Advanced
conda create -n flash-mineru4 python=3.11 -y
conda activate flash-mineru4
pip install "flash-mineru[mineru4]==1.1.0"

# 每张 GPU 常驻一套本地 vLLM 模型栈的 MinerU 4 Advanced
conda create -n flash-mineru4-local python=3.11 -y
conda activate flash-mineru4-local
pip install "flash-mineru[mineru4-local-vllm]==1.1.0"
```

本地 vLLM extra 是 Flash-MinerU 实测过的 CUDA 12 组合，由本项目维护，不代表 MinerU 上游的官方支持声明。加载模型前先确认当前环境：

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

## 2. 准备输入和模型

把一个或多个 PDF 放进 `inputs/`。所有 Worker 都必须能够读取 PDF 和模型路径，并写入输出路径；多机 Ray 集群建议使用共享存储。

```bash
mkdir -p inputs outputs models
```

模型下载始终是显式操作，`import flash_mineru` 不会自动联网。

```python
from flash_mineru import prepare_pipeline

# 下载一个 MinerU 2.5 checkpoint；修改管线名即可选择 Pro 模型。
model = prepare_pipeline(
    "v2.5",
    download=True,
    cache_dir="models/huggingface",
)
print(model)

# 在 MinerU 4 环境中运行，准备 MinerU 4 模型根目录。
model_root = prepare_pipeline(
    "v4-basic",
    model="models/mineru4",
    download=True,
)
print(model_root)
```

离线环境请传入已有本地目录并保持 `download=False`。MinerU 4 Standard/Advanced 除本地小模型根目录外还需要 VLM；下面假设已经按照当前 MinerU 部署方式启动兼容 OpenAI API 的 VLM 服务，地址为 `http://127.0.0.1:18000/v1`。Local/Shared Advanced 会在每个 Ray Actor 内加载 vLLM，不接受 `vlm_server_url`。

## 3. 用同一个脚本运行全部管线

仓库已经提供 [`examples/run_pipeline.py`](../examples/run_pipeline.py)。九条内置管线使用同一程序，只需要调整管线名、模型路径以及可选的 Server 配置；这里也给出完整代码，方便只通过 PyPI 安装的用户直接复制。

```python
import argparse
from pathlib import Path

from flash_mineru import MineruEngine, pipeline_names


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline", required=True, choices=pipeline_names())
    parser.add_argument("--model", required=True, help="Checkpoint 或 MinerU 4 模型根目录")
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--gpus", type=int, default=1)
    parser.add_argument("--vlm-server-url", default="")
    parser.add_argument("--vlm-model", default="")
    return parser.parse_args()


def collect_pdfs(path: Path) -> list[str]:
    pdfs = [path] if path.is_file() else sorted(path.glob("*.pdf"))
    if not pdfs:
        raise SystemExit(f"在 {path} 下没有找到 PDF")
    return [str(pdf.resolve()) for pdf in pdfs]


args = parse_args()

with MineruEngine(
    pipeline_version=args.pipeline,
    model=args.model,
    save_dir=str(args.output),
    replicas=args.gpus,                         # 先用 1，确认后再扩展到每卡一个 Actor。
    num_gpus_per_replica=1.0,
    batch_size=16,                              # 只负责对返回的输出路径分组。
    input_batch_size=24,                        # 每个 RayOrch 输入 batch 接纳的 PDF 数。
    inflight=4,                                 # 允许同时推进的输入 batch 数。
    vlm_server_url=args.vlm_server_url,
    vlm_model=args.vlm_model,
) as engine:
    batches = engine.run(collect_pdfs(args.input))

for batch in batches:
    for output in batch:
        print(output)
```

先用一个 PDF 和一张 GPU 验证：

```bash
python examples/run_pipeline.py \
  --pipeline v2.5 \
  --model /shared/models/MinerU2.5-2509-1.2B \
  --input inputs/example.pdf \
  --output outputs/v2.5 \
  --gpus 1
```

跑通后再把 `--input` 指向目录，并将 `--gpus` 调整为可用 GPU 数量。`MineruEngine` 存活期间 RayOrch 会保持模型 Actor 常驻，将不同 PDF 已就绪的页面或窗口组成 batch，并把结果恢复到原文档顺序。

Engine 会为两条本地 Advanced 管线自动使用 `engine_gpu_util_rate_to_ray_cap=0.1`，其他管线使用 `0.9`。只有确认可用显存后才建议在 `MineruEngine` 中手动覆盖。

## 4. 九条内置管线的运行命令

### MinerU 2.5 系列

使用 `flash-mineru25` 环境。三条管线分别使用对应 checkpoint，其余运行方式相同。

```bash
python examples/run_pipeline.py --pipeline v2.5          --model /shared/models/MinerU2.5-2509-1.2B      --input inputs --output outputs/v2.5          --gpus 4
python examples/run_pipeline.py --pipeline v2.5-pro-2604 --model /shared/models/MinerU2.5-Pro-2604-1.2B --input inputs --output outputs/v2.5-pro-2604 --gpus 4
python examples/run_pipeline.py --pipeline v2.5-pro-2605 --model /shared/models/MinerU2.5-Pro-2605-1.2B --input inputs --output outputs/v2.5-pro-2605 --gpus 4
```

### MinerU 4 Flash 与 Basic

使用 `flash-mineru4` 环境。这两条管线使用本地 MinerU 4 小模型根目录，不需要 VLM endpoint。

```bash
python examples/run_pipeline.py --pipeline v4-flash --model /shared/models/mineru4 --input inputs --output outputs/v4-flash --gpus 4
python examples/run_pipeline.py --pipeline v4-basic --model /shared/models/mineru4 --input inputs --output outputs/v4-basic --gpus 4
```

`v4-flash` 为完整性而保留，但实测负载缺少昂贵且适合 batch 的阶段，速度低于原生 MinerU，因此更适合兼容性实验，而不是默认吞吐展示。

### 外部 VLM 模式的 MinerU 4 Standard 与 Advanced

使用 `flash-mineru4` 环境，并单独启动兼容 OpenAI API 的 MinerU VLM 服务。VLM Server 会占用自己的 GPU；`--gpus` 只控制 Flash-MinerU Actor 副本数。

对于实测 Standard/Advanced 使用的 MinerU 2.5 VLM，可以使用下面的 vLLM 命令：

```bash
CUDA_VISIBLE_DEVICES=0 vllm serve opendatalab/MinerU2.5-2509-1.2B \
  --host 127.0.0.1 \
  --port 18000 \
  --logits-processors mineru_vl_utils:MinerULogitsProcessor
```

Flash-MinerU Actor 使用其余 GPU，例如设置 `CUDA_VISIBLE_DEVICES=1,2,3` 并传入 `--gpus 3`。如果采用其他 Server 实现，需要提供 MinerU 期望的 OpenAI 兼容 endpoint。

```bash
CUDA_VISIBLE_DEVICES=1,2,3 python examples/run_pipeline.py --pipeline v4-standard --model /shared/models/mineru4 --input inputs --output outputs/v4-standard --gpus 3 --vlm-server-url http://127.0.0.1:18000/v1
CUDA_VISIBLE_DEVICES=1,2,3 python examples/run_pipeline.py --pipeline v4-advanced --model /shared/models/mineru4 --input inputs --output outputs/v4-advanced --gpus 3 --vlm-server-url http://127.0.0.1:18000/v1
```

如果 endpoint 暴露多个模型，请额外传入 `--vlm-model MODEL_NAME`。运行 Flash-MinerU 前应先独立验证 endpoint。

### 本地 vLLM 模式的 MinerU 4 Advanced

使用 `flash-mineru4-local` 环境。这两条管线会在每个 GPU Actor 中加载完整模型栈，因此先从 `--gpus 1` 开始。推荐 `v4-advanced-shared`：Infer 与 Finish 两个 DAG 阶段复用同一个常驻 Actor 池。

```bash
python examples/run_pipeline.py --pipeline v4-advanced-local  --model /shared/models/mineru4 --input inputs --output outputs/v4-advanced-local  --gpus 4
python examples/run_pipeline.py --pipeline v4-advanced-shared --model /shared/models/mineru4 --input inputs --output outputs/v4-advanced-shared --gpus 4
```

Engine 会为本地 vLLM 自动选择实测起始值 `engine_gpu_util_rate_to_ray_cap=0.1`。只有确认目标 GPU 仍有足够显存后再提高，并为 Layout、MFR、OCR、Ray 和 CUDA 分配预留空间。

## 5. Ray 集群与独立环境

创建 Engine 前先连接 Ray，并在 Actor 需要使用命名 conda 环境时配置 `runtime_env`：

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

每个 Worker 都必须预先存在这个命名环境。Flash-MinerU 不会通过 Ray working directory 复制数 GB 模型目录，请使用共享存储或保证所有节点挂载相同路径。

## 6. 输出与首次运行检查

MinerU 2.5 输出到 `<output>/<pdf_name>/vlm/<pdf_name>.md`，MinerU 4 输出到 `<output>/<pdf_name>/<flash|basic|standard|advanced>/markdown.md`。首次运行应确认：

1. 每个输入 PDF 都有对应输出目录。
2. 预期 Markdown 文件存在且非空。
3. 处理多个 PDF 时 GPU Actor 保持存活，而不是反复加载模型。
4. 多机环境中所有节点都能解析相同的输入、模型和输出路径。

## 7. 常见问题

| 现象 | 检查项 |
|---|---|
| MinerU import/版本错误 | 激活对应环境，不要混装 `mineru25` 与 `mineru4` extras。 |
| 找不到模型 | 传入已有目录，或在对应 runtime 中调用 `prepare_pipeline(..., download=True)`。 |
| Ray 提示 GPU 不足 | 减少 `--gpus`，检查 `nvidia-smi`，并确认 Ray 集群正确注册 GPU 资源。 |
| Local Advanced Actor 退出或 OOM | 先用一个副本和 `0.1` utilization，确认完整模型栈能放入显存后再扩容。 |
| Standard/Advanced HTTP 请求失败 | 独立验证 VLM endpoint 与模型名；Server 要求时保留 URL 末尾的 `/v1`。 |
| Worker 无法读取文件 | 使用所有 Worker 在同一路径可见的绝对路径。 |

完整的 368 PDF 性能方法和结果见 [BENCHMARK.zh.md](./BENCHMARK.zh.md)。
