import argparse
import importlib.util
import json
import sys
from pathlib import Path

import pytest


BENCHMARK_PATH = Path(__file__).resolve().parents[1] / "Benchmark-versioned.py"
sys.path.insert(0, str(BENCHMARK_PATH.parent))
SPEC = importlib.util.spec_from_file_location("benchmark_versioned", BENCHMARK_PATH)
assert SPEC is not None and SPEC.loader is not None
benchmark_versioned = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = benchmark_versioned
SPEC.loader.exec_module(benchmark_versioned)


@pytest.mark.parametrize(
    ("pipeline", "expected"),
    [
        ("v2.5", 128),
        ("v2.5-pro-2605", 128),
        ("v4-flash", 8),
        ("v4-basic", 32),
        ("v4-standard", 8),
        ("v4-advanced", 8),
        ("v4-advanced-local", 8),
        ("v4-advanced-shared", 16),
    ],
)
def test_flash_cli_uses_pipeline_specific_analyze_batch_default(
    pipeline, expected
):
    args = benchmark_versioned.parser().parse_args(
        [
            "flash",
            "--pipeline",
            pipeline,
            "--pdf-dir",
            "/tmp/pdfs",
            "--output-dir",
            "/tmp/output",
            "--report",
            "/tmp/report.json",
            "--model",
            "/tmp/model",
        ]
    )
    assert args.ocr_batch_size is None
    assert benchmark_versioned.resolve_ocr_batch_size(args) == expected


def test_advanced_shared_defaults_to_small_local_vllm_reservation():
    args = benchmark_versioned.parser().parse_args(
        [
            "flash",
            "--pipeline",
            "v4-advanced-shared",
            "--pdf-dir",
            "/tmp/pdfs",
            "--output-dir",
            "/tmp/output",
            "--report",
            "/tmp/report.json",
            "--model",
            "/tmp/model",
        ]
    )
    assert benchmark_versioned.resolve_gpu_memory_utilization(args) == 0.1


def test_compare_advanced_shared_uses_advanced_output_tier(tmp_path: Path):
    pdf = tmp_path / "input.pdf"
    pdf.write_bytes(b"pdf")
    roots = [tmp_path / "flash", tmp_path / "native"]
    for root in roots:
        output = root / pdf.stem / "advanced"
        output.mkdir(parents=True)
        (output / "markdown.md").write_text("same output", encoding="utf-8")
        (output / "middle_json.json").write_text(
            json.dumps({"pdf_info": [{}]}),
            encoding="utf-8",
        )
        (output / "model_output.json").write_text(
            json.dumps(
                {
                    "pages": [
                        [{"type": "text"}, {"type": "equation"}],
                        [{"type": "text"}],
                    ]
                }
            ),
            encoding="utf-8",
        )

    comparison = benchmark_versioned.compare_outputs(
        roots[0],
        roots[1],
        [pdf],
        pipeline_version="v4-advanced-shared",
        v4_tier=benchmark_versioned.V4_TIERS["v4-advanced-shared"],
    )

    assert comparison["comparable_documents"] == 1
    assert comparison["valid_documents"] == 1
    assert comparison["token_jaccard"]["mean"] == 1.0
    assert comparison["token_multiset_f1"]["mean"] == 1.0
    assert comparison["per_document"][0]["candidate_blocks"] == 3
    assert comparison["per_document"][0]["reference_blocks"] == 3


def test_compare_rejects_failed_or_zero_time_reports():
    valid = {
        "error": None,
        "wall_seconds": 1.0,
        "execution_seconds": 0.5,
        "validation": {"documents": 1, "successful_documents": 1},
    }
    benchmark_versioned.validate_report_for_comparison(
        "Flash", valid, require_execution_time=True
    )

    with pytest.raises(ValueError, match="contains an error"):
        benchmark_versioned.validate_report_for_comparison(
            "Flash",
            valid | {"error": {"type": "RuntimeError", "message": "boom"}},
            require_execution_time=True,
        )
    with pytest.raises(ValueError, match="invalid execution_seconds"):
        benchmark_versioned.validate_report_for_comparison(
            "Flash",
            valid | {"execution_seconds": 0.0},
            require_execution_time=True,
        )
    with pytest.raises(ValueError, match="did not validate every output"):
        benchmark_versioned.validate_report_for_comparison(
            "native",
            valid
            | {
                "validation": {
                    "documents": 2,
                    "successful_documents": 1,
                }
            },
        )


def test_aggregate_v4_stage_profiles(tmp_path: Path):
    pdfs = [tmp_path / "a.pdf", tmp_path / "b.pdf"]
    for pdf in pdfs:
        pdf.write_bytes(b"pdf")
        output = tmp_path / "outputs" / pdf.stem / "basic"
        output.mkdir(parents=True)
        (output / "markdown.md").write_text("ok", encoding="utf-8")
        (output / "flash_mineru.json").write_text(
            json.dumps(
                {
                    "stage_profile": {
                        "stages_seconds": {
                            "pdf.render": 1.0,
                            "pdf.layout": 2.0,
                        },
                        "categories_seconds": {
                            "render": 1.0,
                            "layout": 2.0,
                        },
                    }
                }
            ),
            encoding="utf-8",
        )

    profile = benchmark_versioned.aggregate_v4_stage_profiles(
        tmp_path / "outputs",
        pdfs,
        "basic",
    )

    assert profile["documents"] == 2
    assert profile["missing_documents"] == []
    assert profile["stages_seconds"] == {
        "pdf.layout": 4.0,
        "pdf.render": 2.0,
    }
    assert profile["categories_seconds"] == {
        "layout": 4.0,
        "render": 2.0,
    }


def test_flash_initialization_failure_is_reported_without_division_by_zero(
    monkeypatch, tmp_path: Path
):
    pdf = tmp_path / "input.pdf"
    pdf.write_bytes(b"pdf")
    report = tmp_path / "report.json"
    captured = {}

    class BrokenEngine:
        def __init__(self, **kwargs):
            raise RuntimeError("actor initialization failed")

    class Sampler:
        def __init__(self, interval):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            pass

        def summary(self):
            return {"sample_count": 0, "gpus": []}

    monkeypatch.setattr(benchmark_versioned, "ensure_gpu_count", lambda count: None)
    monkeypatch.setattr(benchmark_versioned, "selected_pdfs", lambda args: [pdf])
    monkeypatch.setattr(
        benchmark_versioned,
        "corpus_manifest",
        lambda pdfs: {"pdfs": 1, "pages": 1, "documents": []},
    )
    monkeypatch.setattr(
        benchmark_versioned,
        "runtime_manifest",
        lambda root: {"packages": {"rayorch": "0.1.0"}},
    )
    monkeypatch.setattr(
        benchmark_versioned,
        "validate_outputs",
        lambda args, pdfs: {
            "documents": 1,
            "successful_documents": 0,
            "pages": 0,
            "failed_documents": [{"stem": "input", "errors": ["missing"]}],
        },
    )
    monkeypatch.setattr(benchmark_versioned, "NvidiaSmiSampler", Sampler)
    monkeypatch.setattr(
        benchmark_versioned,
        "save_benchmark_report",
        lambda config, results, **kwargs: captured.update(results=results),
    )
    import flash_mineru

    monkeypatch.setattr(flash_mineru, "MineruEngine", BrokenEngine)
    args = argparse.Namespace(
        output_dir=tmp_path / "output",
        report=report,
        model="/tmp/model",
        batch_size=1,
        gpus=1,
        gpu_memory_utilization=0.8,
        inflight=1,
        pipeline="v2.5",
        ocr_batch_size=1,
        input_batch_size=1,
        parse_mode="auto",
        window_size=8,
        layout_batch_size=8,
        small_backend="torch",
        vlm_engine="vllm",
        vlm_server_url="",
        vlm_api_key="",
        vlm_model="",
        image_analysis=False,
        gpu_sample_interval=1.0,
    )

    with pytest.raises(RuntimeError, match="actor initialization failed"):
        benchmark_versioned.run_flash(args)

    results = captured["results"]
    assert results["execution_seconds"] == 0.0
    assert results["pdf_per_second"] is None
    assert results["page_per_second"] is None
    assert results["error"]["type"] == "RuntimeError"
    assert results["error"]["message"] == "actor initialization failed"
    assert "RuntimeError: actor initialization failed" in results["error"]["traceback"]
