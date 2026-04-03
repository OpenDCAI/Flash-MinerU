# Benchmark 脚本

简体中文 | [English](./BENCHMARK.md)

在**仓库根目录**执行；需已安装 Flash-MinerU 依赖（Ray、PyTorch、vLLM 等）。Flash 脚本用 `--model` 或环境变量 `FLASH_MINERU_MODEL` 指向本地 VLM 目录。更细的版本说明见主 README 的 Benchmark 折叠区。

**脚本分工：** `Benchmark-flashmineru.py` → `MineruEngineLegacy`（v0.0.4，顺序 batch）；`Benchmark-flashmineru_dag.py` → 默认 `MineruEngine`（v1.0.0，流水线并行）；`Benchmark-mineru.py` → 上游 `mineru` CLI。

**参考跑分环境（README 表格同量级）：** 单机约 **8× A100**，语料 **`test/sample_pdfs`**（约 **368** 个 PDF）。下列墙钟时间为该量级下的**参考值**，随驱动与软件版本会变。

| 方案 | 大约运行时间 |
|------|----------|
| Flash v1.0.0（`…_dag.py`，`--inflight 8`） | ~8.5 min |
| 上游 MinerU 并行 CLI（8 分片） | ~14 min |
| Flash v0.0.4（`…flashmineru.py`） | ~23 min |

---

## 1. Flash v0.0.4 — `test/Benchmark-flashmineru.py`

```bash
cd /path/to/Flash-MinerU
python -u test/Benchmark-flashmineru.py \
  --model /path/to/MinerU2.5-2509-1.2B \
  --pdf-dir test/sample_pdfs \
  --replicas 8 --batch-size 16
```

快速试跑：加 **`--smoke`**（单 PDF、单卡）；抽样多 batch，默认3batch：加 **`--profile`**。

---

## 2. Flash v1.0.0 — `test/Benchmark-flashmineru_dag.py`

```bash
python -u test/Benchmark-flashmineru_dag.py \
  --model /path/to/MinerU2.5-2509-1.2B \
  --pdf-dir test/sample_pdfs \
  --replicas 8 --inflight 8
```

多卡上可把 **`--inflight`** 提到 8；默认一般为 4。同样支持 **`--smoke`** / **`--profile`**。跑完后可能在输出目录生成 **`profile.json`**（Ray timeline）。

---

## 3. 上游 MinerU — `test/Benchmark-mineru.py`

需 **`mineru`** 在 `PATH` 上（或与当前 `python` 同环境的 `bin` 里）。建议加 **`--vlm-model-dir`**，避免多进程重复拉模型。

```bash
python -u test/Benchmark-mineru.py \
  --mode parallel --num-shards 8 \
  --pdf-dir test/sample_pdfs \
  --backend vlm-auto-engine \
  --vlm-model-dir /path/to/MinerU2.5-2509-1.2B
```

快速试跑：**`--smoke --vlm-model-dir …`**（单 PDF、单进程）。
