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

简体中文 | [English](./README.md)

</div>

> 使用 **Ray** 加速开源 PDF 解析项目 **MinerU** 中的 **VLM 推理 Pipeline**

Flash-MinerU 是一个**轻量级、低侵入式**的加速项目，目标是通过 **Ray 的并行 / 分布式能力**，对 **MinerU** 中最耗时的 **VLM（Vision-Language Model）推理阶段**进行并行化加速，从而显著提升 **PDF → Markdown** 的整体处理吞吐能力。

本项目的定位是 **并行化与工程加速器**，而非重新实现 MinerU 的核心算法，设计目标包括：

- **依赖少、安装轻量**
  - 可`pip install flash-mineru`一键安装+运行
  - 已在 **METAX 等国产算力环境**中完成测试，
- **最大程度复用 MinerU 的原有逻辑与数据结构**
  - 保持原算法行为与结果一致性
- **多卡 / 多进程 / 多集群友好**
  - 面向大规模 PDF 批量处理场景设计，轻松scale up!

---

## ✨ Features

- 🚀 **Ray 并行推理**  
  将 PDF 页面 / 图片按 batch 切片，分发至多个 Ray actor 并行执行

- 🧠 **VLM 推理加速**  
  聚焦 MinerU 中的 VLM 推理阶段，当前默认支持基于 **vLLM** 的高吞吐推理

- 🧩 **低侵入式设计**  
  保留 MinerU 原有的中间结构（middle_json）与 Markdown 生成逻辑

---

## 🎯 优化原理（流水线并行）

MinerU 的 PDF→Markdown 是一条 **多阶段流水线**（如渲染页面 → VLM → 写 Markdown）。若每个 batch 必须 **整条链路跑完** 才进入下一批，各阶段会 **互相等待**，时间线上出现 **空泡**，GPU **吃不满**。**Flash-MinerU** 默认的 **`MineruEngine`** 让 **多个逻辑 batch 在阶段之间重叠**：一批在做 VLM 时，另一批可在 pdf2img 或 md 阶段推进，从 **端到端** 提高 **有效算力占用**，且不改变 MinerU 各算子语义。

<table width="100%">
<tr>
<td width="50%" valign="top" align="center">
<strong>左 — 空泡（优化前）</strong><br/>
<em>按 batch 串完再下一批，GPU 有明显等待。</em><br/><br/>
<img src="./docs/bubble.png" alt="时序图：流水线空泡，GPU 未吃满" width="100%" />
</td>
<td width="50%" valign="top" align="center">
<strong>右 — 流水线并行（Flash-MinerU）</strong><br/>
<em>多 batch 重叠，GPU 持续有活干。</em><br/><br/>
<img src="./docs/pipelined.png" alt="时序图：流水线并行，GPU 利用率更好" width="100%" />
</td>
</tr>
</table>

---

## 📦 Installation

### 基础安装（轻量模式）

适用于你已经**手动安装好推理引擎**（如 vLLM），或使用包含完整环境的镜像场景：

```bash
pip install flash-mineru
````

### 安装并启用 vLLM 后端（可选）

如果你希望由 Flash-MinerU 一并安装 vLLM 作为推理后端：

```bash
pip install flash-mineru[vllm]
```

---

## 🚀 Quickstart

### 最简 Python API 示例

```python
from flash_mineru import MineruEngine

# PDF的路径
pdfs = [
    "resnet.pdf",
    "yolo.pdf",
    "text2sql.pdf",
]

engine = MineruEngine(
    model="<path_to_local>/MinerU2.5-2509-1.2B",
    # 模型可从 https://huggingface.co/opendatalab/MinerU2.5-2509-1.2B 下载
    batch_size=2,              # 每个逻辑 batch 内 PDF 数
    replicas=3,                # 并行 vLLM / 模型实例数
    num_gpus_per_replica=0.5, # 每个实例占用的 GPU 显存比例（vLLM KV cache）
    save_dir="outputs_mineru", # 解析结果保存路径
    inflight=4,                # 流水线并行深度（v1.0.0 默认路径；大机器可试 8）
)

# 旧版 v0.0.4 顺序 batch API（弃用）：from flash_mineru import MineruEngineLegacy

results = engine.run(pdfs)
print(results)  # list[list[str]], 输出文件夹的名称
```

### 输出说明

* 每个 PDF 的解析结果会生成在：

  ```
  <save_dir>/<pdf_name>/
  ```

* Markdown 文件默认位于：

  ```
  <save_dir>/<pdf_name>/vlm/<pdf_name>.md
  ```

---

## 📊 Benchmark

**脚本用法：** [简体中文](./docs/BENCHMARK.zh.md) · [English](./docs/BENCHMARK.md)

### 实验结果（368 PDF、单机 8× A100 量级）

| 方案 | 推理配置 | 总耗时 |
|----|----|----|
| Flash-MinerU **v1.0.0** | `MineruEngine`，8 replica，`inflight=8` | **~8.5 min** |
| MinerU（原生） | **手动** 8 个 `mineru` 进程（Benchmark 脚本 **parallel** 模式，每进程一卡，`vlm-auto-engine`） | ~14 min |
| Flash-MinerU **v0.0.4** | `MineruEngineLegacy`，8 replica × 1 GPU，`batch_size=16` | ~23 min |
| MinerU（原生） | vLLM，**单卡** | ~65 min |

命令见 [docs/BENCHMARK.zh.md](./docs/BENCHMARK.zh.md)。

### 结论

- **v1.0.0** 相对「手动 8 进程」基线约 **~1.7×** 速度（约 ~8.5 min vs ~14 min）
- **v0.0.4**（`MineruEngineLegacy`）慢于该 8 进程基线（约 ~23 min），可见流水线并行相对「多进程各起一套模型」的收益
- **单卡 ~65 min** 为同语料量级的对照基线

<details>
<summary><strong>实验设置（展开）</strong></summary>

- **数据集**：23 篇论文 PDF（每篇约 9～37 页）各复制 16 份，共 **368** 个文件；默认目录 `test/sample_pdfs`
- **版本**：MinerU 官方 **v2.7.5**；Flash-MinerU **v0.0.4** = `MineruEngineLegacy`（按 batch 顺序跑各阶段）；**v1.0.0** = `MineruEngine`（流水线并行，默认 API）
- **硬件**：单机 **8 × NVIDIA A100**

</details>

> 注：关注整体吞吐；输出结构与 MinerU 对齐。上游并无成熟的「官方多卡一键并行」，表中 8 进程行来自本仓库 **Benchmark-mineru.py** 的手动分片方式。

---

## 🗺️ Roadmap 未来计划
* [x] Benchmark 脚本与文档 — [docs/BENCHMARK.zh.md](./docs/BENCHMARK.zh.md)
* [ ] 支持更多推理后端（如 sglang）
* [ ] 服务化形态（HTTP API / 任务队列）
* [ ] 示例数据与更完整的文档

---

## 🤝 Acknowledgements / 致敬
* **MinerU**
  本项目基于 MinerU 的整体算法设计与工程实践，对其 VLM 推理 Pipeline 进行并行化加速。
  `mineru_core/` 目录中包含从 MinerU 项目中复制并适配的代码逻辑。
  向 MinerU 的原作者及所有贡献者致以诚挚的敬意与感谢。
  🔗 官方仓库 / 主页：
  [https://github.com/opendatalab/MinerU](https://github.com/opendatalab/MinerU)

* **Ray**
  提供强大的分布式与并行计算抽象，使多 GPU / 多进程编排更加简单可靠。
  🔗 官方网站：
  [https://www.ray.io/](https://www.ray.io/)
  🔗 官方 GitHub：
  [https://github.com/ray-project/ray](https://github.com/ray-project/ray)

* **vLLM**
  提供高吞吐、工程化成熟的推理引擎能力（当前默认推理后端）。
  🔗 官方网站：
  [https://vllm.ai/](https://vllm.ai/)
  🔗 官方 GitHub：
  [https://github.com/vllm-project/vllm](https://github.com/vllm-project/vllm)


---

## 📜 License

**AGPL-3.0**

> 说明：
> 本项目的 `mineru_core/` 目录中包含基于 **MinerU（AGPL-3.0）** 项目的衍生代码。
> 根据 AGPL-3.0 的要求，作为衍生作品，本仓库整体以 **AGPL-3.0** 协议开源发布。
> 详情请参见根目录 `LICENSE` 文件及 `mineru_core/README.md`。


