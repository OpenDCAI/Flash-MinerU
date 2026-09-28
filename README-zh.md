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

简体中文 | [English](./README.md) · [Benchmark](./docs/BENCHMARK.zh.md) · [实验结果](./docs/benchmark-results/)
</div>

> **不丢失文档血缘，让 PDF 理解真正扩展起来。**

Flash-MinerU 是一个由 RayOrch 驱动、按版本组织的 [MinerU](https://github.com/opendatalab/MinerU) 执行层。它把每个 PDF 展开为页面或窗口工作（`1 → M`），将不同文档中已经就绪的任务交给共享 CPU/GPU Actor 批处理，再按正确的文档归属和顺序归并结果（`M → 1`）。

它保留 MinerU 的模型和输出约定。每个受支持的 MinerU runtime/版本都隔离在显式的版本化管线中，应用可以逐条升级，同时保持一套精简的 Python API。

> [!IMPORTANT]
> Flash-MinerU 是 OpenDCAI 独立维护的项目，并非 MinerU 官方发行版。项目在仓库许可证约束下复用并适配 MinerU 运行时组件，但 RayOrch DAG、多 GPU 调度、发布节奏、测试过的依赖组合和性能结果均由 Flash-MinerU 维护。如果问题只在 Flash-MinerU 管线中出现，请在本仓库反馈；如果使用 MinerU 官方执行方式也能复现，再向 MinerU 上游反馈。

```mermaid
flowchart LR
    A[PDF 文档] --> B[有序页面 / 窗口]
    B --> C[CPU 渲染]
    C --> D[共享 GPU Actor 池<br/>跨文档批处理]
    D --> E[恢复血缘与顺序]
    E --> F[Markdown / JSON]
```

## Ray、RayOrch 与 Flash-MinerU 的分工

三个名称对应三层不同职责：

| 层 | 在本项目中的职责 |
|---|---|
| [**MinerU**](https://github.com/opendatalab/MinerU) | 处理页面或窗口，生成文档结果，并定义输出格式和约定。 |
| [**Ray**](https://github.com/ray-project/ray) | 提供物理分布式运行时：Actor、GPU 放置、资源、RPC 和对象存储。 |
| [**RayOrch**](https://github.com/OpenDCAI/RayOrch) | 提供逻辑数据流：显式展开/归并、血缘、就绪判断、跨文档 batching 和有序重建。 |
| **Flash-MinerU** | 将每个受支持的 MinerU 版本接入版本化 RayOrch 管线，并暴露统一的 `MineruEngine` API。 |

Ray 可以运行这些 Actor，但应用仍然需要明确：页面属于哪个 PDF、父项何时完成，以及乱序结果如何恢复。RayOrch 将这些规则放在模型代码之外；Flash-MinerU 再把各阶段填入 MinerU 操作。

### 这里的 `1 → M → 1` 是什么

对一个输入 PDF，流水线包含三个逻辑步骤：

1. **`1 → M` —— 展开：** 将一个 PDF 渲染为数量可变的页面或窗口。每个子项都保留父文档和自己的序号。
2. **批处理就绪子项：** 不同 PDF 中已经就绪的页面或窗口可以进入同一个 CPU/GPU Actor 批次。这个物理批次只是临时组织方式，不改变归属和顺序。
3. **`M → 1` —— 归并：** 按父文档和序号收集完成的子项，再写回一个有序文档结果。某个文档自己的子项就绪后即可完成，不必等待无关文档。

因此，系统可以提高加速器利用率，同时保持文档级结果合同不变：物理调度可以共享，逻辑血缘仍然属于各自文档。

## 为什么需要这一层执行运行时？

模型驱动的文档流水线通常不只受单次模型调用限制：文档长度不同，CPU 与 GPU 阶段推进速度不同，而跨文档组 batch 又不能丢失文档归属和页序。Flash-MinerU 使用 RayOrch 重叠这些阶段、聚合不同文档中已经就绪的任务，并在整个 DAG 中显式保留血缘。

`v4-advanced-shared` 还展示了共享 Actor 复用：`Infer` 与 `Finish` 是两个独立的逻辑 DAG Call，但由同一个四副本物理模型池执行。每个 Actor 只加载一套完整模型栈，中间状态通过 RayOrch Port 传递，不依赖请求再次落到同一副本。在 368 PDF 实测中，它把 `v4-advanced-local` 的执行时间从 1,350.25 秒降到 1,234.60 秒，进一步提升 **9.4%**。

## 实测结果

下表使用同一组 368 个 PDF（7,072 页）和 4× NVIDIA H20。“执行阶段”只统计 Actor 初始化完成后的 `Executor.run`，“冷启动”包含 Ray 启动、模型初始化、执行与清理；输出质量与对应的原生 MinerU 结果比较。

| 管线 | Native | Flash 执行阶段 | 执行阶段加速 | 冷启动加速 | 平均 Jaccard / F1 |
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

所有实验均完成 368/368 个文档和 7,072/7,072 页，没有页数不一致。`v4-flash` 被如实保留为负向结果：该负载缺少昂贵且适合 batch 的模型阶段，因此更细的 DAG 带来了额外开销，而非吞吐收益。完整配置、计时口径、质量校验和 batch 消融见 [Benchmark 文档](./docs/BENCHMARK.zh.md)与[机器可读结果](./docs/benchmark-results/)。

这些数字对应公开的 1.1.0 基准配置，不是对任意 workload 的速度保证。应先按模型和 runtime 兼容性选择管线，再在自己的数据上调节副本数和 batching 参数。

## 如何选择管线

实际使用可以先按四类管线族选择。具体名称采用版本化设计，因为 MinerU 2.5 与 MinerU 4 不能共用同一套依赖环境。

| 管线族 | 管线名称 | 安装 extra | 执行形态 |
|---|---|---|---|
| MinerU 2.5 | `v2.5`、`v2.5-pro-2604`、`v2.5-pro-2605` | `mineru25` | 共享 VLM 池上的跨文档页面 batching |
| MinerU 4 本地阶段 | `v4-flash`、`v4-basic` | `mineru4` | 本地小模型阶段的窗口调度，不需要 VLM endpoint |
| MinerU 4 + VLM 服务 | `v4-standard`、`v4-advanced` | `mineru4` | 本地小模型加共享 HTTP 或本地 VLM |
| MinerU 4 本地 vLLM | `v4-advanced-local`、`v4-advanced-shared` | `mineru4-local-vllm` | 本地模型栈；`shared` 让多个逻辑 Call 复用同一个 Actor 池 |

模型准备、服务要求和每条管线的可运行命令见[管线运行指南](./docs/PIPELINES.zh.md)。

## 安装

MinerU 2.5 与 MinerU 4 依赖不同的大版本运行栈。轻量 Driver 可以只安装基础包；推理环境请选择且只选择一组 runtime extra。

| 安装命令 | 用途 |
|---|---|
| `pip install flash-mineru` | 公共 API、RayOrch 和轻量 Driver |
| `pip install "flash-mineru[mineru25]"` | MinerU 2.5 与 2.5 Pro 管线 |
| `pip install "flash-mineru[mineru4]"` | MinerU 4 小模型与外部 HTTP VLM Server |
| `pip install "flash-mineru[mineru4-local-vllm]"` | MinerU 4 本地 vLLM，包括 `v4-advanced-local` 与 `v4-advanced-shared` |

不要在同一个环境中同时安装 MinerU 2.5 与 MinerU 4 extras。如果一个 Driver 需要运行两代管线，请使用独立 conda 环境和 RayOrch `runtime_env`。`flash-mineru[vllm]` 仅作为旧版 MinerU 2.5 兼容别名保留，新环境不建议使用。

## 准备模型

模型准备是显式操作：导入 Flash-MinerU 不会自动下载 checkpoint。

### 使用已有本地 checkpoint

```python
from flash_mineru import prepare_pipeline

model = prepare_pipeline(
    "v2.5",
    model="/shared/models/MinerU2.5-2509-1.2B",
)
```

### 下载 MinerU 2.5 checkpoint

```python
model = prepare_pipeline(
    "v2.5-pro-2605",             # 使用注册表中的默认 Hugging Face 仓库。
    download=True,                # 只有显式开启才会访问网络。
    cache_dir="/shared/hf-cache",
)
```

当前注册的默认模型是 `opendatalab/MinerU2.5-2509-1.2B`、`opendatalab/MinerU2.5-Pro-2604-1.2B` 和 `opendatalab/MinerU2.5-Pro-2605-1.2B`。

### 准备 MinerU 4 模型根目录

MinerU 4 使用模型根目录，而不是单个 checkpoint 目录。请在 MinerU 4 环境中完成准备，并把目录放到所有 Worker 都能访问的共享存储：

```python
model_root = prepare_pipeline(
    "v4-advanced-shared",
    model="/shared/models/mineru4",
    download=True,
)
```

默认 `download=False`。此时轻量 Driver 可以直接传入已有共享目录，最终的 runtime 与模型校验会在 Actor 启动时执行。配置外部 `vlm_server_url` 后，`v4-standard` 与 `v4-advanced` 本地只需要准备小模型；`v4-advanced-local` 与 `v4-advanced-shared` 必须使用本地 vLLM 模型，不接受 HTTP VLM Server。

## 运行

### 推荐 Python API

```python
from flash_mineru import MineruEngine, prepare_pipeline

pdfs = ["paper-a.pdf", "paper-b.pdf"]
model = prepare_pipeline("v2.5", model="/shared/models/MinerU2.5-2509-1.2B")

with MineruEngine(
    pipeline_version="v2.5",
    model=model,
    save_dir="outputs",
    replicas=4,             # 通常每张 GPU 对应一个模型 Actor。
    batch_size=16,          # 为兼容旧 API，对返回路径进行分组。
    ocr_batch_size=128,     # 一次模型调用聚合的就绪页面数。
    input_batch_size=24,    # 一个 RayOrch 输入 batch 接纳的 PDF 数。
    inflight=4,             # 允许同时推进的输入 batch 数。
) as engine:
    results = engine.run(pdfs)

print(results)
```

上下文管理器会在运行结束后释放常驻 Ray Actor。如果需要复用同一个 Engine 执行多次任务，可以长期持有实例，并在最后调用 `engine.close()`。

如果希望从安装开始直接跑通，或者需要九条内置管线各自可复制的配置，请阅读 **[运行全部管线](./docs/PIPELINES.zh.md)**。建议先用一个 PDF 和 `replicas=1` 验证输出，再把副本数提升到可用 GPU 数量。

### MinerU 4 Advanced 共享 Actor 池

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
    ocr_batch_size=16,                  # 共享 Actor 池的实测默认值。
    input_batch_size=16,
    inflight=3,
    num_gpus_per_replica=1.0,           # Ray 为每个 Actor 预留一张 GPU。
    engine_gpu_util_rate_to_ray_cap=0.1,# 本地 vLLM 的显存利用率配置。
    image_analysis=True,
) as engine:
    results = engine.run(pdfs)
```

### 已有 Ray 集群或独立 Actor 环境

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

对应 conda 环境必须存在于 Worker 节点，并安装 Flash-MinerU 与所选 MinerU runtime。PDF 路径、模型根目录和输出目录也必须能被这些 Worker 访问。

## 管线目录

```python
from flash_mineru import pipeline_names

print(pipeline_names())
```

| 管线 | Runtime | 执行形态 |
|---|---|---|
| `v2.5` | `mineru25` | 在共享 VLM 池上进行跨文档页面 batching |
| `v2.5-pro-2604`, `v2.5-pro-2605` | `mineru25` | 页面 batching 后恢复文档顺序，再执行跨页表格合并 |
| `v4-flash`, `v4-basic` | `mineru4` | 对本地小模型阶段进行窗口调度 |
| `v4-standard`, `v4-advanced` | `mineru4` | 本地小模型加共享 HTTP 或本地 VLM |
| `v4-advanced-local` | `mineru4-local-vllm` | 每个 GPU Actor 常驻一套完整本地模型栈 |
| `v4-advanced-shared` | `mineru4-local-vllm` | 独立 Infer/Finish Call 复用同一个常驻 Actor 池 |

每条实现都独立存放在 `flash_mineru/pipelines/<version>/`，包含自己的 Pipeline、UDF 和模型准备逻辑。这样可以隔离 MinerU 上游版本变化，也让新增管线更容易理解和审查。

表中的 Runtime 表示所需安装 extra，不意味着不同 MinerU 大版本可以混装。模型准备、外部 Server 要求、可运行示例、建议起始参数和常见问题见[管线运行指南](./docs/PIPELINES.zh.md)。

## 输出

MinerU 2.5 的结果默认位于：

```text
<save_dir>/<pdf_name>/vlm/<pdf_name>.md
```

MinerU 4 保留对应 tier 的目录结构：

```text
<save_dir>/<pdf_name>/<flash|basic|standard|advanced>/markdown.md
```

`MineruEngine.run()` 保留历史上的 `list[list[str]]` 返回结构。`MineruEngineLegacy` 继续兼容旧版顺序实现，但新集成应使用 `MineruEngine`。

## 致谢

Flash-MinerU 构建于 [MinerU](https://github.com/opendatalab/MinerU)、[Ray](https://github.com/ray-project/ray)、[RayOrch](https://github.com/OpenDCAI/RayOrch) 和 [vLLM](https://github.com/vllm-project/vllm) 之上，感谢这些项目的作者与贡献者。

## 许可证

Flash-MinerU 基于 MinerU 开发，并包含修改后的 MinerU 源代码。本仓库采用 [MinerU Open Source License](./LICENSE)，即 Apache License 2.0 加附加条款；部署前请仔细阅读其中的商业使用门槛与署名要求。第三方依赖仍适用各自许可证，仓库完整许可证文本见 [`LICENSE`](./LICENSE)。
