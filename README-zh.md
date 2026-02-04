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
[![Downloads](https://static.pepy.tech/badge/flash-mineru)](https://pepy.tech/project/flash-mineru)
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
import ray
from flash_mineru import MineruEngine

ray.init()  # 或 ray.init(address="auto")

pdfs = [
    "resnet.pdf",
    "yolo.pdf",
    "text2sql.pdf",
]

engine = MineruEngine(
    model="<path_to_local>/MinerU2.5-2509-1.2B",
    # 模型可从 https://huggingface.co/opendatalab/MinerU2.5-2509-1.2B 下载
    batch_size=1,              # 单个模型实例内部同时处理的 PDF 数量
    replicas=3,                # 并行启动的 vLLM / 模型实例数量
    num_gpus_per_replica=0.5, # 每个实例占用的 GPU 显存比例（vLLM KV cache）
    save_dir="outputs_mineru", # 解析结果保存路径
)

results = engine.run(pdfs)
print(results)  # list[list[str]]
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

## 🗺️ Roadmap 未来计划
* [ ] Benchmark 脚本（单卡 vs 多 replica 对比）
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


