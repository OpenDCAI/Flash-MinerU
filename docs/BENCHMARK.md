# Benchmark scripts

[简体中文](./BENCHMARK.zh.md) | English

Run from the **repository root** with Flash-MinerU dependencies installed (Ray, PyTorch, vLLM, etc.). Flash scripts need `--model` or `FLASH_MINERU_MODEL` pointing at a local VLM directory. For the narrative benchmark table, see the Benchmark section in [README.md](../README.md).

**Scripts:** `Benchmark-flashmineru.py` → `MineruEngineLegacy` (v0.0.4, sequential batches); `Benchmark-flashmineru_dag.py` → default `MineruEngine` (v1.0.0, pipeline parallelism); `Benchmark-mineru.py` → upstream `mineru` CLI.

**Reference class of setup (same ballpark as the README table):** about **8× A100**, corpus **`test/sample_pdfs`** (~**368** PDFs). Wall times below are **indicative** and depend on drivers and package versions.

| Setup | Approx. wall time |
|--------|-------------------|
| Flash v1.0.0 (`…_dag.py`, `--inflight 8`) | ~8.5 min |
| Upstream MinerU parallel CLI (8 shards) | ~14 min |
| Flash v0.0.4 (`…flashmineru.py`) | ~23 min |

---

## 1. Flash v0.0.4 — `test/Benchmark-flashmineru.py`

```bash
cd /path/to/Flash-MinerU
python -u test/Benchmark-flashmineru.py \
  --model /path/to/MinerU2.5-2509-1.2B \
  --pdf-dir test/sample_pdfs \
  --replicas 8 --batch-size 16
```

**Quick try:** `--smoke` (one PDF, one GPU). **Short multi-batch probe:** `--profile`.

---

## 2. Flash v1.0.0 — `test/Benchmark-flashmineru_dag.py`

```bash
python -u test/Benchmark-flashmineru_dag.py \
  --model /path/to/MinerU2.5-2509-1.2B \
  --pdf-dir test/sample_pdfs \
  --replicas 8 --inflight 8
```

Raise **`--inflight`** (e.g. 8) on large hosts; default is often 4. **`--smoke`** / **`--profile`** work the same way. A **`profile.json`** (Ray timeline) may appear under the output directory.

---

## 3. Upstream MinerU — `test/Benchmark-mineru.py`

**`mineru`** must be on `PATH` (or next to the same Python’s `bin`). Pass **`--vlm-model-dir`** so parallel runs do not each download weights.

```bash
python -u test/Benchmark-mineru.py \
  --mode parallel --num-shards 8 \
  --pdf-dir test/sample_pdfs \
  --backend vlm-auto-engine \
  --vlm-model-dir /path/to/MinerU2.5-2509-1.2B
```

**Quick try:** `--smoke --vlm-model-dir …` (one PDF, single process).
