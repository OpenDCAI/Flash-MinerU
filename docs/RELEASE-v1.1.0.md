# Flash-MinerU v1.1.0

[中文](#中文) · [English](#english)

## 中文

Flash-MinerU 1.1.0 将项目从单条 MinerU 2.5 加速实现扩展为一套基于 RayOrch 的版本化 MinerU Pipeline 集合。本版本依赖已发布的 `rayorch>=0.1.1,<0.2`，在保留 `MineruEngine.run()` 历史返回结构和主要构造参数的同时，为 MinerU 2.5、2.5 Pro 与 MinerU 4 多种执行形态提供统一入口。

### 主要更新

- 新增九条显式注册的 Pipeline：`v2.5`、`v2.5-pro-2604`、`v2.5-pro-2605`、`v4-flash`、`v4-basic`、`v4-standard`、`v4-advanced`、`v4-advanced-local` 与 `v4-advanced-shared`。
- 每条 Pipeline 独立维护 Pipeline、UDF 和模型准备逻辑，降低 MinerU 上游快速演进对其他版本的影响。
- 新增 `pipeline_names()`、`get_pipeline_spec()` 和 `prepare_pipeline()`，模型下载由用户显式触发，导入包不会隐式联网。
- `v4-advanced-shared` 使用 RayOrch 0.1.1 的共享 Actor Pool，让 Infer 与 Finish 两个逻辑阶段复用同一套常驻模型 Actor；在 4× H20、368 PDF/7,072 页实验中，执行时间为 1,234.60 秒，相对同拓扑 Native 为 **1.57×**，相对 `v4-advanced-local` 再提升 **9.4%**。
- MinerU 2.5 三条管线在相同数据集上取得 **2.65×–2.68×** 执行阶段加速；所有九条实验均完成 368/368 文档和 7,072/7,072 页，没有页数丢失。
- 新增分离的 `mineru25`、`mineru4` 与 `mineru4-local-vllm` 安装 extras，并补充九条 Pipeline 的可复制运行指南、多机共享路径要求、输出结构与常见问题。
- 发布 CI 现在校验 GitHub Release tag、源码版本与 wheel metadata 一致后才上传 PyPI。

### 与 MinerU 的关系

Flash-MinerU 是 OpenDCAI 独立维护的 RayOrch 衍生项目，并非 MinerU 官方发行版。项目复用并适配 MinerU 的模型与运行时组件，不替换其模型和主要输出格式；RayOrch DAG、多 GPU 调度、兼容补丁、测试依赖组合和性能结论由 Flash-MinerU 维护。MinerU 4 本地 vLLM extra 是本项目验证过的 CUDA 12 组合，不代表 MinerU 上游的官方支持声明。

### 安装

```bash
pip install "flash-mineru[mineru25]==1.1.0"
# 或
pip install "flash-mineru[mineru4]==1.1.0"
# 或
pip install "flash-mineru[mineru4-local-vllm]==1.1.0"
```

MinerU 2.5 与 MinerU 4 依赖栈不兼容，请不要在同一环境中混装。完整说明见 `docs/PIPELINES.zh.md` 与 `docs/BENCHMARK.zh.md`。

### 兼容性

- `MineruEngine(...)`、`engine.run(pdfs)`、`close()`、上下文管理器和 `runtime_env` 保留。
- `run()` 继续返回 `list[list[str]]`。
- `pdf2img`、`process_img`、`img2md` 兼容别名保留。
- 原顺序执行实现仍可通过 `MineruEngineLegacy` 使用。
- 参数语义需要注意：`batch_size` 现在只控制返回路径分组；模型侧页面/窗口 batch 使用 `ocr_batch_size`。

## English

Flash-MinerU 1.1.0 grows from a single MinerU 2.5 acceleration path into a versioned collection of MinerU pipelines powered by RayOrch. It now depends on the released `rayorch>=0.1.1,<0.2`, preserves the main `MineruEngine` construction and `run()` result contract, and provides one entry point for MinerU 2.5, 2.5 Pro, and several MinerU 4 deployment shapes.

### Highlights

- Nine explicitly registered pipelines: `v2.5`, `v2.5-pro-2604`, `v2.5-pro-2605`, `v4-flash`, `v4-basic`, `v4-standard`, `v4-advanced`, `v4-advanced-local`, and `v4-advanced-shared`.
- Each pipeline owns its Pipeline, UDFs, and preparation logic, isolating fast-moving upstream MinerU generations from one another.
- New `pipeline_names()`, `get_pipeline_spec()`, and `prepare_pipeline()` APIs. Downloads are explicit; importing Flash-MinerU never downloads a model.
- `v4-advanced-shared` uses RayOrch 0.1.1 shared actor pools so separate Infer and Finish calls reuse the same resident model actors. On 4× H20 with 368 PDFs/7,072 pages, it completes execution in 1,234.60 seconds: **1.57×** versus the matched native topology and another **9.4%** faster than `v4-advanced-local`.
- The three MinerU 2.5 pipelines deliver **2.65×–2.68×** execution speedups on the same corpus. All nine experiments finish 368/368 documents and 7,072/7,072 pages without page-count loss.
- Separate `mineru25`, `mineru4`, and `mineru4-local-vllm` extras, plus copy-paste commands for all pipelines, multi-node path requirements, output layouts, and troubleshooting.
- Publishing now verifies that the GitHub Release tag, source version, and wheel metadata agree before uploading to PyPI.

### Relationship with MinerU

Flash-MinerU is independently maintained by OpenDCAI and is not an official MinerU distribution. It reuses and adapts MinerU models and runtime components without replacing their main output formats; Flash-MinerU owns its RayOrch DAGs, multi-GPU scheduling, compatibility shims, tested dependency combinations, and performance claims. The MinerU 4 local-vLLM extra is a Flash-MinerU-tested CUDA 12 combination, not an upstream MinerU support statement.

### Install

```bash
pip install "flash-mineru[mineru25]==1.1.0"
# or
pip install "flash-mineru[mineru4]==1.1.0"
# or
pip install "flash-mineru[mineru4-local-vllm]==1.1.0"
```

Do not install MinerU 2.5 and MinerU 4 extras into the same environment. See `docs/PIPELINES.md` and `docs/BENCHMARK.md` for the full guide.

### Compatibility

- `MineruEngine(...)`, `engine.run(pdfs)`, `close()`, context-manager support, and `runtime_env` remain available.
- `run()` continues to return `list[list[str]]`.
- The `pdf2img`, `process_img`, and `img2md` compatibility aliases remain available.
- The previous sequential implementation remains available as `MineruEngineLegacy`.
- One semantic clarification: `batch_size` now groups returned paths; model-side page/window batching is controlled by `ocr_batch_size`.
