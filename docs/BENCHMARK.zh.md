# Flash-MinerU Benchmark

简体中文 | [English](./BENCHMARK.md)

`test/Benchmark-versioned.py` 是原生 MinerU 与 Flash-MinerU 共用的可复现实验入口。它保证双方使用同一组有序 PDF、同一模型、同一 MinerU 环境、同一解析模式、同一图像分析开关和相同 GPU 数量，并记录语料 SHA-256、软件与 GPU 版本、墙钟时间、吞吐、GPU 采样和输出校验；关键配置不一致时会直接拒绝比较。

## 管线迁移验证

2026 年 9 月 21 日，我们使用同一个真实的 12 页 PDF，分别通过原生 MinerU 和对应的具名 Flash-MinerU/RayOrch 管线，验证了 MinerU 3.4.4 以及当前 MinerU 4.0.5 源码快照（`94828a88d80ab8a72c21956de0604bf14f4cc9fc`）暴露的全部管线。所有运行均成功生成完整文档和 12/12 页输出，原生与 Flash 对比均保持页数和 block 数量一致。

| Flash-MinerU 管线 | 调度粒度 | 原生等价性结果 |
|---|---|---|
| `v2.5` | 页面 | 1/1 有效；token Jaccard 0.9934，token F1 0.9984 |
| `v2.5-pro-2604` | 页面 | 1/1 有效；token Jaccard 0.9913，token F1 0.9986 |
| `v2.5-pro-2605` | 页面 | 1/1 有效；token Jaccard 0.9970，token F1 0.9987 |
| `v4-flash` | TXT 为整文档，OCR 为窗口 | 1/1 有效；Markdown 逐字节一致 |
| `v4-basic` | 有序窗口 | 1/1 有效；Markdown 逐字节一致 |
| `v4-standard` | 有序窗口与布局引导 VLM | 1/1 有效；Markdown 逐字节一致 |
| `v4-advanced` | 有序窗口与整页 VLM | 1/1 有效；页数与 block 结构一致，token Jaccard 0.9988，token F1 0.9996 |

这组验证同时覆盖图构建、真实模型执行、结果落盘和原生输出等价性。单文档耗时只用于诊断，**不作为性能结论**。机器可读证据保存在 [`docs/benchmark-results/all-pipelines-e2e-2026-09-21.json`](./benchmark-results/all-pipelines-e2e-2026-09-21.json)，性能结论仍以下面的 368 PDF 实验为准。

新增的 `v4-advanced-local` 和 `v4-advanced-shared` 部署变体于 2026 年 9 月 22 日使用完整 368 PDF 语料完成验证。两者都用每张 GPU 一套完整常驻模型栈替代共享 HTTP server；`v4-advanced-shared` 进一步把推理与收尾拆成两个逻辑 Call，并让它们复用同一个物理 RayOrch Actor Pool。

## 368 PDF 全管线实测结果

2026 年 9 月 20–22 日，我们在同一组 **368 个 PDF / 7,072 页**语料上完成了各条注册管线的测试。三条 MinerU 2.5 管线、V4 Flash、V4 Basic、V4 Advanced Local 和 V4 Advanced Shared 直接使用 **4× NVIDIA H20**；V4 Standard 和原始 V4 Advanced 的两侧都使用**三张执行 GPU 加一张共享 HTTP VLM server GPU**，VLM server 启动时间不计入任一侧。Flash execution 从 Ray Actor 和模型 ready 后开始计时，Flash cold 则额外包含 Ray 启动、Actor/模型初始化和清理。

| 管线 | Native | Flash execution | Flash cold | 执行阶段加速比 | 冷启动加速比 | Jaccard / F1 |
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

每一行都完整处理了 **368/368 个 PDF 和 7,072/7,072 页**，没有页数不一致。V4 Flash 是一个重要的负向结果：多数 TXT 输入本来就走 MinerU 成本很低的整文档原生路径，没有暴露昂贵且适合合批的模型阶段，因此细粒度 DAG 的调度开销大于收益。V4 Basic 原先在模型边界也存在类似问题：Analyze UDF 虽然收到多个 window，却逐 window 完成后续模型阶段。增强版不改变公开的 `split -> analyze -> assemble` 图，只在同一个 Analyze Actor 内展平当前 ready pages，让 MinerU 的公式、表格和 OCR 阶段真正跨 window 合批，然后恢复各 window 的归属和顺序。Flash execution 因此从 **1,736.74 s 降至 1,550.47 s**，即**减少 10.7%**，相对原生结果也从 **0.95× 提升到 1.06×**，同时页数和 block 数量均保持一致。

`v4-advanced-local` 回答的是与 `v4-advanced` 不同的部署问题。Native 和 Flash 都使用四套本地完整模型栈，每张 H20 常驻 Layout、Two-Step vLLM、MFR 和 OCR，并统一设置 `gpu_memory_utilization=0.1`。Flash 仍把 Two-Step 模型调用保留在单个 Analyze UDF 内，但允许八页 window 独立调度，并让 CPU 渲染与四个 GPU Actor 重叠执行。最终执行阶段加速 **1.44×**，冷启动端到端加速 **1.42×**，节省 578.91 秒；token Jaccard 均值为 **0.9940**，token multiset F1 均值为 **0.9911**，全部页数一致，368 份文档中有 55 份存在 block 数差异，这些差异集中于非确定性的 VLM 输出，而非文档或页面丢失。

`v4-advanced-shared` 直接验证 RayOrch 的 shared-actor 抽象。图中 `Infer（Layout + Two-Step VLM）` 与 `Finish（MFR + OCR + fill）` 是具有独立血缘和 ready queue 的两个逻辑 Call，但复用同一组四个物理 Actor 及其常驻模型栈。相比单一 Analyze Call 的本地管线，执行时间由 **1,350.25 s 降至 1,234.60 s**，吞吐提升 **9.4%**；相对等价 Native 拓扑达到 **1.57× 执行阶段加速**和 **1.56× 冷启动端到端加速**。368 个文档和 7,072 页全部完成；相对 Native 的 token Jaccard 均值为 **0.9945**、token multiset F1 均值为 **0.9906**，54 份文档的 block 数差异来自非确定性的 VLM 解码，而不是血缘或页面丢失。

shared Call 的 batch 消融也说明 batch 并非越大越好。在 48 PDF 调参子集上，从 16 增至 32 后执行时间由 **244.57 s 降至 226.34 s**，但 batch 64 退化到 **363.84 s**；更关键的是，完整 368 PDF 上 batch 32 用时 **1,372.62 s**，比 batch 16 **慢 11.2%**。显存峰值始终约为 15.5–16.0 GiB，因此瓶颈不是 vLLM KV cache 容量，而是过大的 Finish Call 放大了 MFR/OCR 工作块，并增加共享 Pool 的队头阻塞。生产默认值因此继续保持 **16**。

包含全部配置和 V4 Basic 改造前后记录的机器可读结果保存在 [`docs/benchmark-results/all-pipelines-368-2026-09-22.json`](./benchmark-results/all-pipelines-368-2026-09-22.json)。本地完整模型栈和 shared-actor 实验的 GPU、阶段、质量与 batch 消融详情分别保存在 [`v4-advanced-local-4xh20-368-2026-09-22.json`](./benchmark-results/v4-advanced-local-4xh20-368-2026-09-22.json) 与 [`v4-advanced-shared-4xh20-368-2026-09-22.json`](./benchmark-results/v4-advanced-shared-4xh20-368-2026-09-22.json)。

## MinerU 2.5 详细结果

以下结果于 2026 年 9 月 20 日在单机 **4× NVIDIA H20** 上测得，语料为 **368 个 PDF / 7,072 页**。两侧均使用 MinerU 3.4.4、`mineru-vl-utils` 1.2.1、vLLM 0.10.1.1、`parse_mode=auto`、`image_analysis=false` 和同一个本地 MinerU 2.5 模型。原生基线使用四个独立的单卡 MinerU worker，并按页数确定性均衡到每卡恰好 1,768 页；Flash-MinerU 使用四个由 RayOrch 管理的 VLM replica。

| 运行方式 | 计时时间 | PDF/s | page/s |
|---|---:|---:|---:|
| 原生 MinerU，端到端 | 1,177.01 s | 0.313 | 6.008 |
| Flash-MinerU，Actor 初始化后的执行阶段 | 439.72 s | 0.837 | 16.083 |
| Flash-MinerU，冷启动端到端 | 487.25 s | 0.755 | 14.514 |

- **稳定执行阶段加速比：** `1177.01 / 439.72 = 2.68×`
- **冷启动端到端加速比：** `1177.01 / 487.25 = 2.42×`
- 两侧都成功完成 **368/368 个 PDF、7,072/7,072 页**。
- Markdown 对比：token Jaccard 均值 **0.9902**，token multiset F1 均值 **0.9931**，页数完全一致。
- VLM 解码在不同 batching 布局下并非逐字节确定；最低单文件分数来自包含复杂长表格的文档，同一个 PDF 的多个副本在原生 MinerU 内部也会产生差异，因此报告采用语义相似度和结构计数，而不要求 Markdown 字节完全相同。

机器可读的结果保存在 [`docs/benchmark-results/v2.5-4xh20-368-2026-09-20.json`](./benchmark-results/v2.5-4xh20-368-2026-09-20.json)。

## 复现实验

所有命令均从仓库根目录执行。环境中需要安装所选版本的 MinerU、Flash-MinerU 依赖以及已发布的 `rayorch>=0.1.1,<0.2`。

```bash
export MODEL=/path/to/MinerU2.5-2509-1.2B
export PDF_DIR=/path/to/368-pdfs
export PYTHONPATH=/path/to/rayorch/site-packages:.
```

### 1. 原生 MinerU

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

脚本为每张 GPU 启动一个原生 MinerU 进程，并按 PDF 页数做确定性的负载均衡，避免原生基线因为长文档集中在某张卡上而被低估。

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

`startup_seconds` 包含 Ray 启动、Actor 创建、模型加载和 Actor ready；`execution_seconds` 是 `engine.run(...)`；`wall_seconds` 包含构造、执行和清理全过程。

### 3. 校验可比性与输出

```bash
python -u test/Benchmark-versioned.py compare \
  --native-report /tmp/mineru-native.json \
  --flash-report /tmp/flash-mineru.json \
  --report /tmp/mineru-comparison.json
```

比较阶段先检查语料 SHA-256 和关键运行配置，再分别计算纯执行与冷启动端到端加速比，随后逐文件检查输出、页数、block 数量与 Markdown token 相似度。

## 建议的分级验证

完整实验前依次使用 `--limit 1`、`--limit 4`、`--limit 12` 和 `--limit 48`。这样可以用较低成本发现依赖、模型、GPU 清理、输出结构和大 batch 质量问题。

## 其他版本化管线

同一脚本也支持 `v2.5-pro-2604`、`v2.5-pro-2605`、`v4-flash`、`v4-basic`、`v4-standard`、`v4-advanced`、`v4-advanced-local` 和 `v4-advanced-shared`。原生与 Flash 命令必须使用相同的管线模型和后端配置。V4 Standard 与原始 V4 Advanced 还需要保证两侧的 VLM server 拓扑等价，之后得到的数字才可比较。V4 Advanced Local 则要求本地 vLLM 环境和每卡一套完整模型栈；已发布实验使用 `--gpu-memory-utilization 0.1 --window-size 8 --layout-batch-size 8 --ocr-batch-size 8 --input-batch-size 16 --inflight 3`。比较命令会拒绝包含错误、输出校验不完整、墙钟时间为零或 Flash 执行时间为零的报告，避免生成误导性的加速比。

对于 V4 Basic，`--ocr-batch-size` 控制每次送入 Analyze Actor 的 ready window 数量，而不是 MinerU 内部 OCR detector 的 batch size。发布结果使用 `--window-size 8 --ocr-batch-size 32`：八页窗口保留足够细的调度粒度，同时允许最多 32 个 ready window 的页面进入一次本地模型调用。48 PDF 调参实验中，增大到 16 页窗口反而更慢，因此正式 368 PDF 实验保留八页窗口。

如需复现每卡一套完整模型栈的 Advanced 对比，请沿用上面的 `native`、`flash`、`compare` 三步命令，并统一加入 `--pipeline v4-advanced-local --image-analysis --small-backend torch --vlm-engine vllm --gpu-memory-utilization 0.1`；Flash 一侧再加入 `--batch-size 16 --input-batch-size 16 --inflight 3 --ocr-batch-size 8 --window-size 8 --layout-batch-size 8`。两侧都必须使用四张 GPU 和同一个 MinerU 4 模型根目录。

如需复现 shared-actor 变体，把 Flash 管线改为 `v4-advanced-shared`，并使用 `--ocr-batch-size 16`；对应的 Native 基线仍是上述每卡一套完整模型栈的 Advanced 工作负载。这里的 `--ocr-batch-size` 表示任一 shared model Call 最多接收的 ready window 数，而不仅是 OCR 库内部的 batch。完整语料应保持已验证的 16：32 虽然在 48 PDF 子集上更快，但在 368 PDF 上发生退化，64 则产生严重队头阻塞。

旧的 `Benchmark-mineru.py`、`Benchmark-flashmineru.py` 和 `Benchmark-flashmineru_dag.py` 暂时保留用于历史流程，但新的公开结果应统一使用 `Benchmark-versioned.py`。

## 运行时安装与隔离

三条 MinerU 2.5 管线使用 `pip install "flash-mineru[mineru25]"`；MinerU 4 搭配独立 HTTP VLM server 时使用 `pip install "flash-mineru[mineru4]"`；需要本地 MinerU 4 vLLM 后端时使用 `pip install "flash-mineru[mineru4-local-vllm]"`。MinerU 2.5 与 MinerU 4 对 `mineru-vl-utils` 和 Transformers 的大版本要求冲突，因此有意放在不同环境中。本地 vLLM extra 会明确选择 Flash-MinerU 已验证的 CUDA 12 路径（`MinerU 4.0.x` 与 `vLLM 0.10.1.1`），而不是使用 MinerU 当前可能解析到 CUDA 13 vLLM wheel 的 `full` extra；这是 Flash-MinerU 自身维护的兼容组合，不应理解为 MinerU 上游的官方支持声明。

如果 driver 自身没有安装 MinerU 4，请把已经准备好的模型根目录放在所有 worker 均可访问的共享存储上，并向 `MineruEngine` 传入 `runtime_env={"conda": "mineru4"}`。默认 `prepare=False` 时，driver 只解析这个已存在的共享路径，真正的 MinerU 运行时校验会在 Ray Actor 初始化时完成。模型下载仍然必须显式触发，并应在匹配的 Actor 环境中于 benchmark 开始前完成。
