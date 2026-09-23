import os
import subprocess
import sys
import types
from contextlib import contextmanager
from pathlib import Path

import numpy as np
import pytest


def _physical_calls(compiled):
    """Return per-Call pool/dispatch views across RayOrch 0.1 plan revisions."""

    calls = list(compiled.logical.calls)
    pools = [compiled.plan.pool(call) for call in calls]
    dispatches = [
        compiled.plan.dispatch(call)
        if hasattr(compiled.plan, "dispatch")
        else compiled.plan.pool(call)
        for call in calls
    ]
    return pools, dispatches

from flash_mineru.dag_pipeline import FlashMinerRayOrchPipeline
from flash_mineru._vllm_compat import (
    register_transformers_5_compatibility,
)
from flash_mineru.pipelines import (
    get_pipeline_spec,
    pipeline_names,
    prepare_pipeline,
)
from flash_mineru.pipelines.mineru_v2_5.pipeline import MinerU25Pipeline
from flash_mineru.pipelines.mineru_v2_5.udfs import (
    MinerU25AssembleDocument,
    MinerU25PdfStem,
    MinerU25PdfToPages,
    MinerU25VlmPage,
)
from flash_mineru.pipelines.mineru_v2_5_pro_2604.pipeline import (
    MinerU25Pro2604Pipeline,
)
from flash_mineru.pipelines.mineru_v2_5_pro_2604.udfs import (
    MinerU25Pro2604AssembleDocument,
    MinerU25Pro2604PdfStem,
    MinerU25Pro2604PdfToPages,
    MinerU25Pro2604VlmPage,
)
from flash_mineru.pipelines.mineru_v2_5_pro_2605.pipeline import (
    MinerU25Pro2605Pipeline,
)
from flash_mineru.pipelines.mineru_v2_5_pro_2605.udfs import (
    MinerU25Pro2605AssembleDocument,
    MinerU25Pro2605PdfStem,
    MinerU25Pro2605PdfToPages,
    MinerU25Pro2605VlmPage,
)
from flash_mineru.pipelines.mineru_v4_advanced.pipeline import (
    MinerU4AdvancedPipeline,
)
from flash_mineru.pipelines.mineru_v4_advanced.udfs import (
    MinerU4AdvancedAnalyzeWindow,
    MinerU4AdvancedAssembleDocument,
    MinerU4AdvancedRenderWindow,
    MinerU4AdvancedSplitPdf,
)
from flash_mineru.pipelines.mineru_v4_advanced_local.pipeline import (
    MinerU4AdvancedLocalPipeline,
)
from flash_mineru.pipelines.mineru_v4_advanced_local.udfs import (
    MinerU4AdvancedLocalAnalyzeWindow,
    MinerU4AdvancedLocalAssembleDocument,
    MinerU4AdvancedLocalRenderWindow,
    MinerU4AdvancedLocalSplitPdf,
)
from flash_mineru.pipelines.mineru_v4_advanced_shared.pipeline import (
    MinerU4AdvancedSharedPipeline,
)
from flash_mineru.pipelines.mineru_v4_advanced_shared.udfs import (
    MinerU4AdvancedSharedAssembleDocument,
    MinerU4AdvancedSharedModelStack,
    MinerU4AdvancedSharedRenderWindow,
    MinerU4AdvancedSharedSplitPdf,
)
from flash_mineru.pipelines.mineru_v4_basic.pipeline import MinerU4BasicPipeline
from flash_mineru.pipelines.mineru_v4_basic.udfs import (
    MinerU4BasicAnalyzeWindow,
    MinerU4BasicAssembleDocument,
    MinerU4BasicRenderWindow,
    MinerU4BasicSplitPdf,
)
from flash_mineru.pipelines.mineru_v4_flash.pipeline import MinerU4FlashPipeline
from flash_mineru.pipelines.mineru_v4_flash.udfs import (
    MinerU4FlashAnalyzeWindow,
    MinerU4FlashAssembleDocument,
    MinerU4FlashSplitPdf,
)
from flash_mineru.pipelines.mineru_v4_standard.pipeline import (
    MinerU4StandardPipeline,
)
from flash_mineru.pipelines.mineru_v4_standard.udfs import (
    MinerU4StandardAnalyzeWindow,
    MinerU4StandardAssembleDocument,
    MinerU4StandardRenderWindow,
    MinerU4StandardSplitPdf,
)
from rayorch._program.logical import ExpandOrigin, ReduceOrigin


def test_registry_has_explicit_versioned_pipelines():
    assert pipeline_names() == (
        "v2.5",
        "v2.5-pro-2604",
        "v2.5-pro-2605",
        "v4-flash",
        "v4-basic",
        "v4-standard",
        "v4-advanced",
        "v4-advanced-local",
        "v4-advanced-shared",
    )
    assert get_pipeline_spec("2.5") is get_pipeline_spec("v2.5")
    assert get_pipeline_spec("mineru2.5").pipeline_cls is MinerU25Pipeline
    assert (
        get_pipeline_spec("v2.5-pro-2604").pipeline_cls
        is MinerU25Pro2604Pipeline
    )
    assert (
        get_pipeline_spec("v2.5-pro-2605").pipeline_cls
        is MinerU25Pro2605Pipeline
    )
    assert get_pipeline_spec("mineru4-flash").pipeline_cls is MinerU4FlashPipeline
    assert get_pipeline_spec("4-basic").pipeline_cls is MinerU4BasicPipeline
    assert (
        get_pipeline_spec("4-standard").pipeline_cls
        is MinerU4StandardPipeline
    )
    assert (
        get_pipeline_spec("4-advanced").pipeline_cls
        is MinerU4AdvancedPipeline
    )
    assert (
        get_pipeline_spec("4-advanced-local").pipeline_cls
        is MinerU4AdvancedLocalPipeline
    )
    assert (
        get_pipeline_spec("4-advanced-shared").pipeline_cls
        is MinerU4AdvancedSharedPipeline
    )
    assert get_pipeline_spec("v2.5-pro").name == "v2.5-pro-2605"
    with pytest.raises(ValueError, match="unsupported MinerU pipeline"):
        get_pipeline_spec("latest")


def test_every_canonical_pipeline_has_unique_public_types():
    specs = [get_pipeline_spec(name) for name in pipeline_names()]
    assert len({spec.name for spec in specs}) == len(specs)
    assert len({spec.pipeline_cls for spec in specs}) == len(specs)
    assert len({spec.prepare for spec in specs}) == len(specs)
    assert all(
        spec.pipeline_cls.__module__.startswith("flash_mineru.pipelines.")
        for spec in specs
    )


def test_v4_advanced_local_has_one_complete_stack_per_gpu_actor():
    pipeline = MinerU4AdvancedLocalPipeline(
        output_dir="/tmp/flash-mineru-test",
        model="/tmp/model",
        replicas=4,
        ocr_batch_size=16,
        gpu_memory_utilization=0.1,
    )
    compiled = pipeline.compile()
    targets = [spec.udf.target for spec in compiled.logical.calls.values()]
    pools, dispatches = _physical_calls(compiled)

    assert targets == [
        MinerU4AdvancedLocalSplitPdf,
        MinerU4AdvancedLocalRenderWindow,
        MinerU4AdvancedLocalAnalyzeWindow,
        MinerU4AdvancedLocalAssembleDocument,
    ]
    assert [pool.replicas for pool in pools] == [4, 4, 4, 4]
    assert [dict(pool.ray_options).get("num_gpus", 0) for pool in pools] == [
        0,
        0,
        1.0,
        0,
    ]
    assert [dispatch.batch_size for dispatch in dispatches] == [1, 1, 16, 4]


def test_v4_advanced_shared_reuses_one_model_actor_pool():
    pipeline = MinerU4AdvancedSharedPipeline(
        output_dir="/tmp/flash-mineru-test",
        model="/tmp/model",
        replicas=4,
        ocr_batch_size=16,
        gpu_memory_utilization=0.1,
    )
    compiled = pipeline.compile()
    calls = list(compiled.logical.calls)
    targets = [compiled.logical.calls[call].udf.target for call in calls]
    pools, dispatches = _physical_calls(compiled)

    assert targets == [
        MinerU4AdvancedSharedSplitPdf,
        MinerU4AdvancedSharedRenderWindow,
        MinerU4AdvancedSharedModelStack,
        MinerU4AdvancedSharedModelStack,
        MinerU4AdvancedSharedAssembleDocument,
    ]
    model_calls = calls[2:4]
    assert len({compiled.plan.dispatch(call).pool for call in model_calls}) == 1
    assert len(compiled.plan.actor_pools) == 4
    assert [
        dict(compiled.plan.input_layouts_by_call[call].static_kwargs)["stage"]
        for call in model_calls
    ] == ["infer", "finish"]
    assert [dispatch.batch_size for dispatch in dispatches] == [
        1, 1, 16, 16, 4
    ]
    assert [dict(pool.ray_options).get("num_gpus", 0) for pool in pools] == [
        0, 0, 1.0, 1.0, 0
    ]


def test_v4_advanced_local_rejects_external_vlm_server():
    with pytest.raises(ValueError, match="actor-local VLM"):
        MinerU4AdvancedLocalPipeline(
            output_dir="/tmp/flash-mineru-test",
            model="/tmp/model",
            vlm_server_url="http://127.0.0.1:18000/v1",
        )


def test_local_vllm_compatibility_hook_is_idempotent():
    register_transformers_5_compatibility()
    register_transformers_5_compatibility()

    import transformers
    from transformers import PreTrainedTokenizerBase

    if int(transformers.__version__.split(".", 1)[0]) < 5:
        # Transformers 4 already provides the compatibility surface.
        assert hasattr(PreTrainedTokenizerBase, "all_special_tokens_extended")
        return

    from transformers.models.qwen2_vl.configuration_qwen2_vl import (
        Qwen2VLConfig,
    )
    from transformers.models.qwen2_vl.image_processing_qwen2_vl import (
        Qwen2VLImageProcessor,
    )

    assert hasattr(PreTrainedTokenizerBase, "all_special_tokens_extended")
    assert hasattr(Qwen2VLImageProcessor, "min_pixels")
    assert hasattr(Qwen2VLImageProcessor, "max_pixels")
    assert hasattr(Qwen2VLConfig, "hidden_size")
    assert hasattr(Qwen2VLConfig, "rope_theta")
    assert hasattr(Qwen2VLConfig, "rope_parameters")


def test_local_vllm_compatibility_ignores_broken_optional_torchaudio(monkeypatch):
    import flash_mineru._vllm_compat as compatibility

    real_import_module = compatibility.importlib.import_module

    def import_module(name, package=None):
        if name == "torchaudio":
            raise RuntimeError("CUDA mismatch")
        return real_import_module(name, package)

    monkeypatch.setattr(compatibility.importlib, "import_module", import_module)

    from transformers import utils as transformers_utils
    from transformers.utils import import_utils

    original_public = transformers_utils.is_torchaudio_available
    original_internal = import_utils.is_torchaudio_available
    try:
        compatibility._ignore_broken_optional_torchaudio()
        assert transformers_utils.is_torchaudio_available() is False
        assert import_utils.is_torchaudio_available() is False
    finally:
        transformers_utils.is_torchaudio_available = original_public
        import_utils.is_torchaudio_available = original_internal


def test_prepare_accepts_existing_model_without_downloading(tmp_path: Path):
    model = tmp_path / "model"
    model.mkdir()
    assert prepare_pipeline("v2.5", model=model) == str(model.resolve())


def test_prepare_requires_explicit_download_for_repo_id():
    with pytest.raises(FileNotFoundError, match="download=True"):
        prepare_pipeline("v2.5", model="opendatalab/not-a-local-path")


def test_v4_prepare_forwards_explicit_runtime_backends(monkeypatch):
    received = {}

    def fake_prepare(**kwargs):
        received.update(kwargs)
        return "/tmp/models"

    monkeypatch.setattr(
        "flash_mineru.pipelines.mineru_v4_standard.prepare."
        "prepare_mineru_4_models",
        fake_prepare,
    )
    assert (
        prepare_pipeline(
            "v4-standard",
            model="/tmp/models",
            small_backend="onnx",
            vlm_engine="llama-cpp",
            vlm_server_url="http://127.0.0.1:8080/v1",
            source="modelscope",
        )
        == "/tmp/models"
    )
    assert received == {
        "model_root": "/tmp/models",
        "tier": "standard",
        "download": False,
        "small_backend": "onnx",
        "vlm_engine": "llama-cpp",
        "vlm_server_url": "http://127.0.0.1:8080/v1",
        "source": "modelscope",
    }


def test_v4_prepare_does_not_require_local_vlm_for_http_backend(
    monkeypatch, tmp_path: Path
):
    checked = []

    class Repo:
        def __init__(self, name):
            self.name = name

    class Result:
        ready = True
        missing_paths = []

    monkeypatch.setattr(
        "flash_mineru.pipelines._shared.mineru_v4.require_mineru_4",
        lambda: "4.0.4",
    )
    mineru = types.ModuleType("mineru")
    model = types.ModuleType("mineru.model")
    registry = types.ModuleType("mineru.model.registry")
    download = types.ModuleType("mineru.model.download")
    registry.small_model_repo = lambda backend: Repo(f"small-{backend}")
    registry.vlm_model_repo = lambda engine: Repo(f"vlm-{engine}")
    download.verify_model_repo = (
        lambda repo: checked.append(repo.name) or Result()
    )
    monkeypatch.setitem(sys.modules, "mineru", mineru)
    monkeypatch.setitem(sys.modules, "mineru.model", model)
    monkeypatch.setitem(sys.modules, "mineru.model.registry", registry)
    monkeypatch.setitem(sys.modules, "mineru.model.download", download)

    assert prepare_pipeline(
        "v4-standard",
        model=tmp_path,
        small_backend="onnx",
        vlm_engine="vllm",
        vlm_server_url="http://127.0.0.1:18000/v1",
    ) == str(tmp_path.resolve())
    assert checked == ["small-onnx"]


def test_v4_prepare_defers_runtime_validation_to_cross_environment_actor(
    monkeypatch, tmp_path: Path
):
    model_root = tmp_path / "shared-mineru4-models"
    model_root.mkdir()

    def missing_on_driver():
        raise ImportError("MinerU 4 is installed only in the actor environment")

    monkeypatch.setattr(
        "flash_mineru.pipelines._shared.mineru_v4.require_mineru_4",
        missing_on_driver,
    )

    assert prepare_pipeline(
        "v4-standard",
        model=model_root,
        download=False,
    ) == str(model_root.resolve())


def test_v4_prepare_still_rejects_missing_shared_model_root(monkeypatch, tmp_path):
    def missing_on_driver():
        raise ImportError("MinerU 4 is installed only in the actor environment")

    monkeypatch.setattr(
        "flash_mineru.pipelines._shared.mineru_v4.require_mineru_4",
        missing_on_driver,
    )

    with pytest.raises(ImportError, match="actor environment"):
        prepare_pipeline(
            "v4-standard",
            model=tmp_path / "missing-model-root",
            download=False,
        )


def test_v25_pipeline_is_page_grained_and_has_four_actor_pools():
    pipeline = MinerU25Pipeline(
        output_dir="/tmp/flash-mineru-test",
        model="/tmp/model",
        replicas=4,
        ocr_batch_size=128,
    )
    compiled = pipeline.compile()
    targets = [spec.udf.target for spec in compiled.logical.calls.values()]
    pools, dispatches = _physical_calls(compiled)

    assert targets == [
        MinerU25PdfToPages,
        MinerU25VlmPage,
        MinerU25PdfStem,
        MinerU25AssembleDocument,
    ]
    assert [dispatch.batch_size for dispatch in dispatches] == [1, 128, 32, 4]
    assert [pool.replicas for pool in pools] == [4, 4, 1, 4]
    assert sum(
        isinstance(spec.origin, ExpandOrigin)
        for spec in compiled.logical.ports.values()
    ) == 1
    assert sum(
        isinstance(spec.origin, ReduceOrigin)
        for spec in compiled.logical.ports.values()
    ) == 2


def test_compatibility_pipeline_selects_v25():
    pipeline = FlashMinerRayOrchPipeline(
        model="/tmp/model",
        replicas=2,
        num_gpus_per_replica=1.0,
        save_dir="/tmp/output",
        engine_gpu_util_rate_to_ray_cap=0.8,
    )
    assert isinstance(pipeline, MinerU25Pipeline)
    assert pipeline.pdf2img is pipeline.render
    assert pipeline.process_img is pipeline.ocr
    assert pipeline.img2md is pipeline.assemble


@pytest.mark.parametrize(
    ("pipeline_cls", "targets"),
    [
        (
            MinerU25Pro2604Pipeline,
            [
                MinerU25Pro2604PdfToPages,
                MinerU25Pro2604VlmPage,
                MinerU25Pro2604PdfStem,
                MinerU25Pro2604AssembleDocument,
            ],
        ),
        (
            MinerU25Pro2605Pipeline,
            [
                MinerU25Pro2605PdfToPages,
                MinerU25Pro2605VlmPage,
                MinerU25Pro2605PdfStem,
                MinerU25Pro2605AssembleDocument,
            ],
        ),
    ],
)
def test_each_pro_pipeline_owns_its_udfs(pipeline_cls, targets):
    pipeline = pipeline_cls(
        output_dir="/tmp/flash-mineru-test",
        model="/tmp/model",
        replicas=4,
        ocr_batch_size=128,
    )
    compiled = pipeline.compile()
    actual = [spec.udf.target for spec in compiled.logical.calls.values()]
    assert actual == targets
    package = pipeline_cls.__module__.rsplit(".", 1)[0]
    assert all(target.__module__.startswith(package) for target in actual)


@pytest.mark.parametrize(
    ("pipeline_cls", "targets"),
    [
        (
            MinerU4FlashPipeline,
            [
                MinerU4FlashSplitPdf,
                MinerU4FlashAnalyzeWindow,
                MinerU4FlashAssembleDocument,
            ],
        ),
        (
            MinerU4BasicPipeline,
            [
                MinerU4BasicSplitPdf,
                MinerU4BasicRenderWindow,
                MinerU4BasicAnalyzeWindow,
                MinerU4BasicAssembleDocument,
            ],
        ),
        (
            MinerU4StandardPipeline,
            [
                MinerU4StandardSplitPdf,
                MinerU4StandardRenderWindow,
                MinerU4StandardAnalyzeWindow,
                MinerU4StandardAssembleDocument,
            ],
        ),
        (
            MinerU4AdvancedPipeline,
            [
                MinerU4AdvancedSplitPdf,
                MinerU4AdvancedRenderWindow,
                MinerU4AdvancedAnalyzeWindow,
                MinerU4AdvancedAssembleDocument,
            ],
        ),
    ],
)
def test_each_v4_pipeline_is_window_grained_and_owns_its_udfs(
    pipeline_cls, targets
):
    pipeline = pipeline_cls(
        output_dir="/tmp/flash-mineru-test",
        model="/tmp/model",
        replicas=4,
        ocr_batch_size=8,
    )
    compiled = pipeline.compile()
    actual = [spec.udf.target for spec in compiled.logical.calls.values()]
    pools, dispatches = _physical_calls(compiled)

    assert actual == targets
    package = pipeline_cls.__module__.rsplit(".", 1)[0]
    assert all(target.__module__.startswith(package) for target in actual)
    expected_stages = 4 if pipeline_cls is not MinerU4FlashPipeline else 3
    assert [pool.replicas for pool in pools] == [4] * expected_stages
    if expected_stages == 4:
        assert [dispatch.batch_size for dispatch in dispatches] == [1, 1, 8, 4]
        assert [
            dict(pool.ray_options).get("num_gpus", 0) for pool in pools
        ] == [0, 0, 1.0, 0]
    else:
        assert [dispatch.batch_size for dispatch in dispatches] == [1, 8, 4]
        assert [
            dict(pool.ray_options).get("num_gpus", 0) for pool in pools
        ] == [0, 1.0, 0]
    assert sum(
        isinstance(spec.origin, ExpandOrigin)
        for spec in compiled.logical.ports.values()
    ) == 1
    assert sum(
        isinstance(spec.origin, ReduceOrigin)
        for spec in compiled.logical.ports.values()
    ) == 1


def test_v4_pipeline_applies_runtime_env_to_every_actor_pool():
    runtime_env = {"conda": "mineru4"}
    pipeline = MinerU4StandardPipeline(
        output_dir="/tmp/flash-mineru-test",
        model="/tmp/model",
        runtime_env=runtime_env,
    )

    compiled = pipeline.compile()
    pools, _ = _physical_calls(compiled)
    assert all(
        dict(pool.ray_options)["runtime_env"] == runtime_env for pool in pools
    )


def test_v4_basic_direct_pipeline_uses_batched_analyze_default():
    pipeline = MinerU4BasicPipeline(
        output_dir="/tmp/flash-mineru-test",
        model="/tmp/model",
    )

    compiled = pipeline.compile()
    _, dispatches = _physical_calls(compiled)

    assert [dispatch.batch_size for dispatch in dispatches] == [1, 1, 32, 4]


def test_v4_model_pipelines_expose_cpu_render_before_gpu_analyze():
    for pipeline_cls in (
        MinerU4BasicPipeline,
        MinerU4StandardPipeline,
        MinerU4AdvancedPipeline,
    ):
        pipeline = pipeline_cls(
            output_dir="/tmp/flash-mineru-test",
            model="/tmp/model",
            replicas=3,
            layout_batch_size=6,
            stage_options={"render": {"replicas": 5, "num_cpus": 2}},
        )

        calls = list(pipeline.compile().logical.calls.values())
        compiled = pipeline.compile()
        pools, _ = _physical_calls(compiled)

        assert calls[1].udf.target.__name__.endswith("RenderWindow")
        assert calls[2].udf.target.__name__.endswith("AnalyzeWindow")
        assert [pool.replicas for pool in pools] == [3, 5, 3, 3]
        assert dict(pools[1].ray_options)["num_cpus"] == 2
        assert dict(pools[1].ray_options).get("num_gpus", 0) == 0
        assert dict(pools[2].ray_options)["num_gpus"] == 1.0
        analyze_init = dict(calls[2].udf.init_kwargs)
        assert analyze_init["rendered_input"] is True
        assert analyze_init["layout_batch_size"] == 6


def test_v4_render_window_returns_serializable_numpy_pages(monkeypatch):
    closed = []

    class FakeImage:
        mode = "RGB"

        def __array__(self, dtype=None, copy=None):
            value = np.arange(18, dtype=np.uint8).reshape(2, 3, 3)
            return value.astype(dtype) if dtype is not None else value

        def close(self):
            closed.append(True)

    images = types.ModuleType("mineru.backend.analysis.pdf.images")
    images.load_images_from_pdf_core = lambda **kwargs: [
        {"img_pil": FakeImage(), "scale": 2.0}
    ]
    monkeypatch.setitem(sys.modules, "mineru.backend.analysis.pdf.images", images)

    renderer = MinerU4BasicRenderWindow(render_dpi=200)
    output = renderer.run(
        [
            {
                "pdf_path": __file__,
                "start": 0,
                "end": 0,
                "page_count": 1,
                "window_index": 0,
                "window_count": 1,
                "parse_mode": "ocr",
                "native_txt": False,
            }
        ]
    )[0]

    assert output["rendered_pages"][0]["pixels"].shape == (2, 3, 3)
    assert output["rendered_pages"][0]["pixels"].dtype == np.uint8
    assert output["rendered_pages"][0]["scale"] == 2.0
    assert output["stage_timings_s"]["pdf.render"] >= 0
    assert closed == [True]


def test_v4_analyze_batches_layout_across_rendered_windows(monkeypatch):
    calls = []
    acquired = []
    released = []

    class FakeImage:
        def __init__(self, array):
            self.array = array

        def close(self):
            pass

    class FakeDocument:
        def __init__(self, _value):
            self.pages = ["p0", "p1", "p2"]

        def __getitem__(self, index):
            return self.pages[index]

        def close(self):
            pass

    class WindowInputs:
        def __init__(self, *args):
            (
                self.window,
                self.images_list,
                self.window_pages,
                self.images_pil_list,
                self.np_images,
                self.images_layout_res,
                self.vl_style_layout_blocks,
                self.high_vlm_blocks,
                self.accepted_native_tables,
                self.page_text_geometries,
                self.page_vector_geometries,
            ) = args

        def close(self):
            pass

    @contextmanager
    def passthrough_stage(_device):
        yield

    class Layout:
        def batch_predict(self, images, batch_size):
            calls.append((list(images), batch_size))
            return [f"layout-{index}" for index in range(len(images))]

    modules = {
        "PIL": types.ModuleType("PIL"),
        "docvortex": types.ModuleType("docvortex"),
        "docvortex.document": types.ModuleType("docvortex.document"),
        "docvortex.document.pdf": types.ModuleType("docvortex.document.pdf"),
        "mineru": types.ModuleType("mineru"),
        "mineru.backend": types.ModuleType("mineru.backend"),
        "mineru.backend.analysis": types.ModuleType("mineru.backend.analysis"),
        "mineru.backend.analysis.pdf": types.ModuleType(
            "mineru.backend.analysis.pdf"
        ),
        "mineru.backend.analysis.pdf.layout": types.ModuleType(
            "mineru.backend.analysis.pdf.layout"
        ),
        "mineru.backend.analysis.pdf.tables": types.ModuleType(
            "mineru.backend.analysis.pdf.tables"
        ),
        "mineru.backend.analysis.pdf.window": types.ModuleType(
            "mineru.backend.analysis.pdf.window"
        ),
        "mineru.model": types.ModuleType("mineru.model"),
        "mineru.model.runtime": types.ModuleType("mineru.model.runtime"),
        "mineru.model.runtime.execution": types.ModuleType(
            "mineru.model.runtime.execution"
        ),
        "mineru.model.runtime.memory": types.ModuleType(
            "mineru.model.runtime.memory"
        ),
    }
    modules["PIL"].Image = types.SimpleNamespace(
        fromarray=lambda value: FakeImage(value)
    )
    modules["docvortex.document.pdf"].PDFDocument = FakeDocument
    modules["mineru.backend.analysis.pdf.layout"]._collect_table_items = (
        lambda layouts, images: []
    )
    modules["mineru.backend.analysis.pdf.layout"]._build_vl_style_layout_blocks = (
        lambda layouts, images: list(layouts)
    )
    modules[
        "mineru.backend.analysis.pdf.tables"
    ]._apply_native_txt_table_priority = lambda *args, **kwargs: None
    modules[
        "mineru.backend.analysis.pdf.tables"
    ]._apply_table_orientations = lambda *args, **kwargs: None
    modules[
        "mineru.backend.analysis.pdf.tables"
    ]._split_native_high_table_blocks = lambda blocks: (blocks, [])
    modules["mineru.backend.analysis.pdf.window"]._ProcessingWindow = (
        lambda index, total, start, end: types.SimpleNamespace(
            index=index, total=total, start=start, end=end
        )
    )
    modules["mineru.backend.analysis.pdf.window"]._WindowInputs = WindowInputs
    modules["mineru.backend.analysis.pdf.window"]._get_window_pdf_pages = (
        lambda document, window: document.pages[
            window.start : window.end + 1
        ]
    )
    modules["mineru.backend.analysis.pdf.window"]._log_processing_window = (
        lambda *args: None
    )
    modules["mineru.model.runtime.execution"].acquire_document = (
        lambda device: acquired.append(device)
    )
    modules["mineru.model.runtime.execution"].local_model_stage = passthrough_stage
    modules["mineru.model.runtime.execution"].release_document = (
        lambda device, cleanup: released.append(device)
    )
    modules["mineru.model.runtime.memory"].clean_memory = lambda device: None
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)

    analyzer = MinerU4BasicAnalyzeWindow.__new__(MinerU4BasicAnalyzeWindow)
    analyzer.effort = "medium"
    analyzer.layout_batch_size = 6
    analyzer._hybrid_model = types.SimpleNamespace(
        device="cuda",
        layout_model=Layout(),
    )
    analyzer._hybrid = lambda: analyzer._hybrid_model
    first = np.zeros((2, 3, 3), dtype=np.uint8)
    second = np.ones((2, 3, 3), dtype=np.uint8)
    records = [
        {
            "pdf_path": __file__,
            "start": 0,
            "end": 0,
            "page_count": 3,
            "window_index": 0,
            "window_count": 2,
            "parse_mode": "ocr",
            "native_txt": False,
            "rendered_pages": [{"pixels": first, "scale": 1.0}],
            "stage_timings_s": {"pdf.render": 0.2},
        },
        {
            "pdf_path": __file__,
            "start": 1,
            "end": 1,
            "page_count": 3,
            "window_index": 1,
            "window_count": 2,
            "parse_mode": "ocr",
            "native_txt": False,
            "rendered_pages": [{"pixels": second, "scale": 1.0}],
            "stage_timings_s": {"pdf.render": 0.3},
        },
    ]

    prepared = analyzer._prepare_rendered_batch(records)
    try:
        assert len(calls) == 1
        assert len(calls[0][0]) == 2
        assert calls[0][1] == 6
        assert prepared[0]["state"].images_layout_res == ["layout-0"]
        assert prepared[1]["state"].images_layout_res == ["layout-1"]
        assert prepared[0]["stage_timings_s"]["pdf.render"] == 0.2
        assert prepared[1]["stage_timings_s"]["pdf.render"] == 0.3
        assert "pdf.layout" in prepared[0]["stage_timings_s"]
        assert "pdf.layout" in prepared[1]["stage_timings_s"]
        assert acquired == ["cuda", "cuda"]
    finally:
        for item in prepared:
            item["state"].close()
            item["document"].close()
            released.append("cuda")


@pytest.mark.parametrize(
    ("pipeline_version", "expected_batch_size"),
    [
        ("v2.5", 128),
        ("v4-flash", 8),
        ("v4-basic", 32),
        ("v4-standard", 8),
        ("v4-advanced", 8),
        ("v4-advanced-local", 8),
        ("v4-advanced-shared", 16),
    ],
)
def test_engine_selects_pipeline_specific_analyze_batch_default(
    monkeypatch, pipeline_version, expected_batch_size
):
    captured = {}

    monkeypatch.setattr(
        "flash_mineru.dag_pipeline.prepare_pipeline",
        lambda *args, **kwargs: "/tmp/model",
    )
    monkeypatch.setattr(
        "flash_mineru.dag_pipeline.create_pipeline",
        lambda version, **kwargs: captured.update(
            version=version, kwargs=kwargs
        )
        or object(),
    )
    monkeypatch.setattr(
        "flash_mineru.dag_pipeline.Executor",
        lambda pipeline: types.SimpleNamespace(close=lambda: None),
    )

    from flash_mineru.dag_pipeline import MineruRayOrchDagEngine

    MineruRayOrchDagEngine(
        pipeline_version=pipeline_version,
        model="/tmp/model",
        save_dir="/tmp/output",
        batch_size=1,
        replicas=1,
    )

    assert captured["version"] == pipeline_version
    assert captured["kwargs"]["ocr_batch_size"] == expected_batch_size
    if pipeline_version == "v4-flash":
        assert "layout_batch_size" not in captured["kwargs"]
    expected_utilization = (
        0.1
        if pipeline_version in {"v4-advanced-local", "v4-advanced-shared"}
        else 0.9
    )
    assert captured["kwargs"]["gpu_memory_utilization"] == expected_utilization


def test_v4_basic_batches_local_models_across_windows(monkeypatch):
    calls = []
    attachments = []

    @contextmanager
    def passthrough_stage(_device):
        yield

    @contextmanager
    def passthrough_timer(_name):
        yield

    def process_text_and_formulas(
        images_list,
        window_pages,
        model_list,
        parse_mode,
        effort,
        local_model_context,
        images_layout_res,
        page_text_geometries,
        *,
        page_vector_geometries,
        np_images,
    ):
        calls.append(
            {
                "images": list(images_list),
                "pages": list(window_pages),
                "models": list(model_list),
                "mode": parse_mode,
                "effort": effort,
                "layouts": list(images_layout_res),
                "text_geometries": list(page_text_geometries),
                "vector_geometries": list(page_vector_geometries),
                "np_images": list(np_images),
                "hybrid": local_model_context,
            }
        )
        return [{"result": page} for page in window_pages]

    def apply_seal_ocr(hybrid, model_list, np_images):
        assert hybrid.device == "cuda"
        assert len(model_list) == len(np_images) == 3

    def attach_visual_block_images(model_list, images_list, page_start_index=0):
        attachments.append(
            (list(model_list), list(images_list), page_start_index)
        )

    modules = {
        "docvortex": types.ModuleType("docvortex"),
        "docvortex.document": types.ModuleType("docvortex.document"),
        "docvortex.document.pdf": types.ModuleType("docvortex.document.pdf"),
        "docvortex.document.pdf.visuals": types.ModuleType(
            "docvortex.document.pdf.visuals"
        ),
        "mineru": types.ModuleType("mineru"),
        "mineru.backend": types.ModuleType("mineru.backend"),
        "mineru.backend.analysis": types.ModuleType("mineru.backend.analysis"),
        "mineru.backend.analysis.pdf": types.ModuleType(
            "mineru.backend.analysis.pdf"
        ),
        "mineru.backend.analysis.pdf.ocr": types.ModuleType(
            "mineru.backend.analysis.pdf.ocr"
        ),
        "mineru.backend.analysis.pdf.window": types.ModuleType(
            "mineru.backend.analysis.pdf.window"
        ),
        "mineru.model": types.ModuleType("mineru.model"),
        "mineru.model.runtime": types.ModuleType("mineru.model.runtime"),
        "mineru.model.runtime.execution": types.ModuleType(
            "mineru.model.runtime.execution"
        ),
        "mineru.utils": types.ModuleType("mineru.utils"),
        "mineru.utils.timing": types.ModuleType("mineru.utils.timing"),
    }
    modules[
        "docvortex.document.pdf.visuals"
    ].attach_visual_block_images = attach_visual_block_images
    modules["mineru.backend.analysis.pdf.ocr"]._apply_seal_ocr = apply_seal_ocr
    modules[
        "mineru.backend.analysis.pdf.window"
    ]._process_text_and_formulas = process_text_and_formulas
    modules["mineru.model.runtime.execution"].local_model_stage = passthrough_stage
    modules["mineru.utils.timing"].stage_timer = passthrough_timer
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)

    def state(prefix, start, count):
        return types.SimpleNamespace(
            images_list=[f"{prefix}-image-{i}" for i in range(count)],
            window_pages=[f"{prefix}-page-{i}" for i in range(count)],
            vl_style_layout_blocks=[
                f"{prefix}-model-{i}" for i in range(count)
            ],
            np_images=[f"{prefix}-np-{i}" for i in range(count)],
            images_layout_res=[
                f"{prefix}-layout-{i}" for i in range(count)
            ],
            page_text_geometries=[
                f"{prefix}-text-{i}" for i in range(count)
            ],
            page_vector_geometries=[
                f"{prefix}-vector-{i}" for i in range(count)
            ],
            window=types.SimpleNamespace(start=start),
        )

    first = state("first", 0, 2)
    second = state("second", 8, 1)
    prepared = [
        {
            "output_index": 1,
            "record": {
                "pdf_path": "/tmp/first.pdf",
                "start": 0,
                "end": 1,
                "parse_mode": "txt",
            },
            "state": first,
            "elapsed_s": 1.0,
        },
        {
            "output_index": 0,
            "record": {
                "pdf_path": "/tmp/second.pdf",
                "start": 8,
                "end": 8,
                "parse_mode": "txt",
            },
            "state": second,
            "elapsed_s": 2.0,
        },
    ]
    outputs = [None, None]
    analyzer = MinerU4BasicAnalyzeWindow.__new__(MinerU4BasicAnalyzeWindow)
    analyzer._finish_medium_batch(
        prepared,
        outputs,
        types.SimpleNamespace(device="cuda"),
    )

    assert len(calls) == 1
    assert calls[0]["pages"] == [
        "first-page-0",
        "first-page-1",
        "second-page-0",
    ]
    assert calls[0]["mode"] == "txt"
    assert calls[0]["effort"] == "medium"
    assert [attachment[2] for attachment in attachments] == [0, 8]
    assert outputs[0]["pages"] == [{"result": "second-page-0"}]
    assert outputs[1]["pages"] == [
        {"result": "first-page-0"},
        {"result": "first-page-1"},
    ]


def test_v4_advanced_batches_finalize_across_windows(monkeypatch):
    processed_batches = []
    attachments = []

    @contextmanager
    def passthrough_stage(_device):
        yield

    def process_text_and_formulas(
        images_list,
        window_pages,
        model_list,
        parse_mode,
        effort,
        local_model_context,
        images_layout_res,
        page_text_geometries,
        *,
        page_vector_geometries,
        np_images,
    ):
        processed_batches.append(
            {
                "images": list(images_list),
                "pages": list(window_pages),
                "models": list(model_list),
                "mode": parse_mode,
                "effort": effort,
                "layouts": list(images_layout_res),
                "text_geometries": list(page_text_geometries),
                "vector_geometries": list(page_vector_geometries),
                "np_images": list(np_images),
                "hybrid": local_model_context,
            }
        )
        return model_list

    modules = {
        "docvortex": types.ModuleType("docvortex"),
        "docvortex.assets": types.ModuleType("docvortex.assets"),
        "docvortex.document": types.ModuleType("docvortex.document"),
        "docvortex.document.pdf": types.ModuleType("docvortex.document.pdf"),
        "docvortex.document.pdf.visuals": types.ModuleType(
            "docvortex.document.pdf.visuals"
        ),
        "mineru": types.ModuleType("mineru"),
        "mineru.backend": types.ModuleType("mineru.backend"),
        "mineru.backend.analysis": types.ModuleType("mineru.backend.analysis"),
        "mineru.backend.analysis.pdf": types.ModuleType(
            "mineru.backend.analysis.pdf"
        ),
        "mineru.backend.analysis.pdf.layout": types.ModuleType(
            "mineru.backend.analysis.pdf.layout"
        ),
        "mineru.backend.analysis.pdf.normalization": types.ModuleType(
            "mineru.backend.analysis.pdf.normalization"
        ),
        "mineru.backend.analysis.pdf.visual_containers": types.ModuleType(
            "mineru.backend.analysis.pdf.visual_containers"
        ),
        "mineru.backend.analysis.pdf.window": types.ModuleType(
            "mineru.backend.analysis.pdf.window"
        ),
        "mineru.model": types.ModuleType("mineru.model"),
        "mineru.model.runtime": types.ModuleType("mineru.model.runtime"),
        "mineru.model.runtime.execution": types.ModuleType(
            "mineru.model.runtime.execution"
        ),
    }
    modules["docvortex.assets"].image_size = lambda image: image.size
    modules[
        "docvortex.document.pdf.visuals"
    ].attach_visual_block_images = lambda models, images, page_start_index=0: (
        attachments.append((list(models), list(images), page_start_index))
    )
    modules[
        "mineru.backend.analysis.pdf.layout"
    ]._convert_vlm_results_to_model_list = lambda values: [
        [dict(block) for block in page] for page in values
    ]
    modules[
        "mineru.backend.analysis.pdf.layout"
    ]._normalize_xhigh_vlm_blocks = lambda values: None
    modules[
        "mineru.backend.analysis.pdf.normalization"
    ]._apply_layout_title_split = lambda *args: None
    modules[
        "mineru.backend.analysis.pdf.visual_containers"
    ].supplement_missing_image_block_containers = lambda *args: None
    modules[
        "mineru.backend.analysis.pdf.window"
    ]._process_text_and_formulas = process_text_and_formulas
    modules["mineru.backend.analysis.pdf.window"].stage_timer = (
        lambda _name: contextmanager(lambda: (yield))()
    )
    modules["mineru.model.runtime.execution"].local_model_stage = (
        passthrough_stage
    )
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)

    def state(prefix, start, count):
        return types.SimpleNamespace(
            images_list=[f"{prefix}-image-{i}" for i in range(count)],
            images_pil_list=[
                types.SimpleNamespace(size=(100, 200)) for _ in range(count)
            ],
            window_pages=[f"{prefix}-page-{i}" for i in range(count)],
            np_images=[f"{prefix}-np-{i}" for i in range(count)],
            images_layout_res=[
                f"{prefix}-layout-{i}" for i in range(count)
            ],
            vl_style_layout_blocks=[
                f"{prefix}-local-{i}" for i in range(count)
            ],
            page_text_geometries=[
                f"{prefix}-text-{i}" for i in range(count)
            ],
            page_vector_geometries=[
                f"{prefix}-vector-{i}" for i in range(count)
            ],
            window=types.SimpleNamespace(start=start),
        )

    first = state("first", 0, 2)
    second = state("second", 8, 1)
    selected = [
        {
            "output_index": 1,
            "record": {
                "pdf_path": "/tmp/first.pdf",
                "start": 0,
                "end": 1,
                "parse_mode": "txt",
            },
            "state": first,
            "elapsed_s": 1.0,
            "stage_timings_s": {},
        },
        {
            "output_index": 0,
            "record": {
                "pdf_path": "/tmp/second.pdf",
                "start": 8,
                "end": 8,
                "parse_mode": "txt",
            },
            "state": second,
            "elapsed_s": 2.0,
            "stage_timings_s": {},
        },
    ]
    inferred = [
        [{"type": "text", "content": "first-0"}],
        [{"type": "text", "content": "first-1"}],
        [{"type": "text", "content": "second-0"}],
    ]
    outputs = [None, None]
    analyzer = MinerU4AdvancedAnalyzeWindow.__new__(
        MinerU4AdvancedAnalyzeWindow
    )
    analyzer._finish_xhigh_batch(
        selected,
        inferred,
        outputs,
        types.SimpleNamespace(device="cuda"),
        infer_elapsed_s=3.0,
    )

    assert len(processed_batches) == 1
    assert processed_batches[0]["pages"] == [
        "first-page-0",
        "first-page-1",
        "second-page-0",
    ]
    assert processed_batches[0]["mode"] == "txt"
    assert processed_batches[0]["effort"] == "xhigh"
    assert [attachment[2] for attachment in attachments] == [0, 8]
    assert outputs[0]["pages"] == [[{"type": "text", "content": "second-0"}]]
    assert outputs[1]["pages"] == [
        [{"type": "text", "content": "first-0"}],
        [{"type": "text", "content": "first-1"}],
    ]


@pytest.mark.parametrize(
    "pipeline_cls",
    [
        MinerU4FlashPipeline,
        MinerU4BasicPipeline,
        MinerU4StandardPipeline,
        MinerU4AdvancedPipeline,
    ],
)
@pytest.mark.parametrize(
    "overrides",
    [
        {"render_dpi": 144},
        {"start_page_id": 1},
        {"end_page_id": 2},
    ],
)
def test_v4_pipeline_rejects_unimplemented_page_selection(
    pipeline_cls, overrides
):
    with pytest.raises(ValueError, match="render_dpi=200"):
        pipeline_cls(
            output_dir="/tmp/flash-mineru-test",
            model="/tmp/model",
            **overrides,
        )


def test_public_import_does_not_load_mineru_model_stacks():
    script = """
import json
import sys
import flash_mineru
print(json.dumps({
    "mineru": any(name == "mineru" or name.startswith("mineru.") for name in sys.modules),
    "vllm": any(name == "vllm" or name.startswith("vllm.") for name in sys.modules),
    "docvortex": any(name == "docvortex" or name.startswith("docvortex.") for name in sys.modules),
}))
"""
    env = dict(os.environ)
    repo_root = str(Path(__file__).resolve().parents[2])
    inherited_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = os.pathsep.join(
        part for part in (repo_root, inherited_pythonpath) if part
    )
    completed = subprocess.run(
        [sys.executable, "-c", script],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    assert completed.stdout.strip() == (
        '{"mineru": false, "vllm": false, "docvortex": false}'
    )
