"""Low-level MinerU 4 bridge used by the four public tier pipelines.

Public Pipeline and UDF classes stay in their version directories.  This
module only owns mechanics that are identical for every MinerU 4 tier:
splitting a PDF into serializable window records, executing one mixed batch of
windows through MinerU's official stages, and saving the official result.
"""

from __future__ import annotations

import json
import os
import time
from collections import defaultdict
from collections.abc import Iterator
from contextlib import contextmanager
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any, Literal

MinerU4Effort = Literal["flash", "medium", "high", "xhigh"]
ParseMode = Literal["auto", "txt", "ocr"]

_PROFILE_GROUPS = {
    "render": ("pdf.render",),
    "layout": ("pdf.layout",),
    "table": (
        "pdf.table_orientation",
        "pdf.native_tables",
        "pdf.table_recognition",
    ),
    "mfr": ("pdf.formulas", "pdf.formula_numbers"),
    "ocr": (
        "pdf.ocr_detection",
        "pdf.ocr_recognition",
        "pdf.text_fill",
        "pdf.seals",
    ),
    "vlm": ("pdf.vlm",),
    "assemble": ("pdf.assemble",),
}


class _StageRecorder:
    """Collect cumulative actor-stage wall time without changing MinerU output."""

    def __init__(self) -> None:
        self.seconds: dict[str, float] = defaultdict(float)

    @contextmanager
    def timer(self, stage: str) -> Iterator[None]:
        started_at = time.perf_counter()
        try:
            yield
        finally:
            self.seconds[stage] += time.perf_counter() - started_at


@contextmanager
def _capture_window_stage_timers(recorder: _StageRecorder) -> Iterator[None]:
    """Route MinerU window timers into one actor-local recorder."""

    import mineru.backend.analysis.pdf.window as window_module

    original = getattr(window_module, "stage_timer", None)
    window_module.stage_timer = recorder.timer
    try:
        yield
    finally:
        if original is None:
            del window_module.stage_timer
        else:
            window_module.stage_timer = original


def _merge_stage_timings(
    target: dict[str, float],
    source: dict[str, float] | None,
) -> None:
    for stage, seconds in (source or {}).items():
        target[stage] = target.get(stage, 0.0) + float(seconds)


def _allocate_stage_timings(
    items: list[dict[str, Any]],
    timings: dict[str, float],
    *,
    page_counts: list[int],
) -> None:
    """Attribute one shared model call by page count without double counting."""

    total_pages = sum(page_counts)
    for item, pages in zip(items, page_counts, strict=True):
        share = pages / total_pages if total_pages else 0.0
        profile = item.setdefault("stage_timings_s", {})
        for stage, seconds in timings.items():
            profile[stage] = profile.get(stage, 0.0) + seconds * share


def _profile_summary(raw: dict[str, float]) -> dict[str, Any]:
    normalized = {
        stage: round(float(seconds), 6)
        for stage, seconds in sorted(raw.items())
    }
    categories = {
        category: round(sum(normalized.get(stage, 0.0) for stage in stages), 6)
        for category, stages in _PROFILE_GROUPS.items()
    }
    return {
        "kind": "cumulative_actor_stage_wall_time",
        "stages_seconds": normalized,
        "categories_seconds": categories,
    }


def require_mineru_4() -> str:
    """Return the installed MinerU version or raise an actionable error."""

    try:
        installed = version("mineru")
    except PackageNotFoundError as exc:
        raise ImportError(
            "MinerU 4 pipelines require `mineru>=4.0.4,<4.1` in their actor "
            "environment."
        ) from exc
    if not installed.startswith("4.0."):
        raise ImportError(
            "MinerU 4 pipelines currently target the 4.0 runtime contract; "
            f"found mineru=={installed}."
        )
    return installed


def configure_mineru_4(
    *,
    model_base_dir: str,
    small_backend: str,
    vlm_engine: str,
    vlm_server_url: str,
    vlm_api_key: str,
    vlm_model: str,
    vlm_max_concurrency: int,
) -> None:
    """Set MinerU configuration before importing any MinerU runtime module."""

    os.environ["MINERU_MODEL_BASE_DIR"] = str(
        Path(model_base_dir).expanduser().resolve()
    )
    os.environ["MINERU_MODEL_SMALL_BACKEND"] = small_backend
    os.environ["MINERU_MODEL_VLM_ENGINE"] = vlm_engine
    os.environ["MINERU_MODEL_VLM_SERVER_URL"] = vlm_server_url
    os.environ["MINERU_MODEL_VLM_API_KEY"] = vlm_api_key
    os.environ["MINERU_MODEL_VLM_MODEL"] = vlm_model
    os.environ["MINERU_MODEL_VLM_MAX_CONCURRENCY"] = str(vlm_max_concurrency)


def enable_local_vllm_compatibility() -> None:
    """Bridge MinerU 4's Transformers 5 runtime to CUDA-12 vLLM 0.10.

    MinerU 4's local Layout model requires Transformers 5, while vLLM 0.10
    still reads a removed tokenizer property. The compatibility hook is also
    registered as a vLLM general plugin so spawned EngineCore processes apply
    the same narrow patch before constructing their tokenizer cache.
    """

    os.environ.setdefault("VLLM_USE_V1", "1")

    from flash_mineru._vllm_compat import (
        register_transformers_5_compatibility,
    )

    register_transformers_5_compatibility()


def prepare_mineru_4_models(
    *,
    model_root: str | Path,
    tier: str,
    download: bool,
    small_backend: str = "torch",
    vlm_engine: str = "vllm",
    vlm_server_url: str = "",
    source: str | None = None,
) -> str:
    """Verify or explicitly download the model repositories for one tier."""

    root = Path(model_root).expanduser().resolve()
    os.environ["MINERU_MODEL_BASE_DIR"] = str(root)
    os.environ["MINERU_MODEL_SMALL_BACKEND"] = small_backend
    os.environ["MINERU_MODEL_VLM_ENGINE"] = vlm_engine
    if not download:
        try:
            require_mineru_4()
        except ImportError:
            # A driver may intentionally submit this pipeline to actors in a
            # different runtime_env.  When a shared model root already exists,
            # defer MinerU's authoritative runtime/model validation to actor
            # initialization instead of requiring MinerU 4 on the driver.
            if root.is_dir():
                return str(root)
            raise

        from mineru.model.download import verify_model_repo
        from mineru.model.registry import small_model_repo, vlm_model_repo

        repos = [small_model_repo(small_backend)]
        if tier in {"standard", "advanced"} and not vlm_server_url:
            repos.append(vlm_model_repo(vlm_engine))
        missing = []
        for repo in repos:
            result = verify_model_repo(repo)
            if not result.ready:
                missing.append(f"{repo.name}: {', '.join(result.missing_paths)}")
        if missing:
            detail = "; ".join(missing)
            raise FileNotFoundError(
                f"MinerU 4 models are not ready under {root}: {detail}. "
                "Pass download=True to prepare them explicitly."
            )
        return str(root)

    require_mineru_4()
    from mineru.model.registry import small_model_repo, vlm_model_repo

    repos = [small_model_repo(small_backend)]
    # Advanced is hosted by a Standard deployment and therefore uses the same
    # VLM model repository.
    if tier in {"standard", "advanced"} and not vlm_server_url:
        repos.append(vlm_model_repo(vlm_engine))

    for repo in repos:
        repo.ensure(source=source)
    return str(root)


def split_pdf_windows(
    pdf_paths: list[str],
    *,
    effort: MinerU4Effort,
    parse_mode: ParseMode,
    window_size: int,
) -> list[list[dict[str, Any]]]:
    """Describe each PDF as ordered, serializable processing windows."""

    require_mineru_4()
    if window_size <= 0:
        raise ValueError("window_size must be positive")

    from docvortex.document.pdf import PDFDocument

    groups: list[list[dict[str, Any]]] = []
    for raw_path in pdf_paths:
        path = str(Path(raw_path).expanduser().resolve())
        file_bytes = Path(path).read_bytes()
        with PDFDocument(file_bytes) as document:
            resolved_mode = document.classify() if parse_mode == "auto" else parse_mode
            page_count = document.page_count
        if resolved_mode not in {"txt", "ocr"}:
            raise ValueError(f"unsupported MinerU parse mode: {resolved_mode}")

        # Flash TXT uses DocVortex's native whole-document PDF model and has no
        # expensive page model stage to batch.  Keep one lineage child so this
        # branch remains honest rather than pretending to be page-parallel.
        native_txt = effort == "flash" and resolved_mode == "txt"
        step = page_count if native_txt and page_count else window_size
        starts = list(range(0, page_count, step))
        groups.append(
            [
                {
                    "pdf_path": path,
                    "start": start,
                    "end": min(page_count - 1, start + step - 1),
                    "page_count": page_count,
                    "window_index": index,
                    "window_count": len(starts),
                    "parse_mode": resolved_mode,
                    "native_txt": native_txt,
                }
                for index, start in enumerate(starts)
            ]
        )
    return groups


class MinerU4RenderWindow:
    """Render PDF windows on CPU actors into Ray-serializable RGB arrays."""

    def __init__(self, *, render_dpi: int = 200) -> None:
        if render_dpi <= 0:
            raise ValueError("render_dpi must be positive")
        self.render_dpi = render_dpi

    def run(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        from mineru.backend.analysis.pdf.images import (
            load_images_from_pdf_core,
        )
        import numpy as np

        outputs = []
        for record in records:
            started_at = time.perf_counter()
            if record["native_txt"]:
                from docvortex.analyzers.native import PdfModel
                from docvortex.document.pdf import PDFDocument
                from docvortex.document.pdf.visuals import (
                    attach_visual_block_images_from_pdf,
                )

                file_bytes = Path(record["pdf_path"]).read_bytes()
                with PDFDocument(file_bytes) as document:
                    pages = PdfModel().predict(document)
                    attach_visual_block_images_from_pdf(document, pages)
                output = dict(record)
                output["native_pages"] = pages
                output["stage_timings_s"] = {
                    "pdf.render": time.perf_counter() - started_at
                }
                outputs.append(output)
                continue
            images_list = load_images_from_pdf_core(
                pdf_bytes=Path(record["pdf_path"]).read_bytes(),
                dpi=self.render_dpi,
                start_page_id=int(record["start"]),
                end_page_id=int(record["end"]),
                image_type="pil_img",
            )
            try:
                rendered_pages = [
                    {
                        "pixels": np.asarray(image["img_pil"]).copy(),
                        "mode": image["img_pil"].mode,
                        "scale": image.get("scale"),
                    }
                    for image in images_list
                ]
            finally:
                for image in images_list:
                    image["img_pil"].close()
            output = dict(record)
            output["rendered_pages"] = rendered_pages
            output["stage_timings_s"] = {
                "pdf.render": time.perf_counter() - started_at
            }
            outputs.append(output)
        return outputs


class MinerU4WindowAnalyzer:
    """Execute official MinerU 4 window stages over a cross-document batch."""

    def __init__(
        self,
        *,
        effort: MinerU4Effort,
        model_base_dir: str,
        small_backend: str,
        vlm_engine: str,
        vlm_server_url: str,
        vlm_api_key: str,
        vlm_model: str,
        vlm_max_concurrency: int,
        gpu_memory_utilization: float,
        image_analysis: bool,
        rendered_input: bool = False,
        layout_batch_size: int = 8,
    ) -> None:
        configure_mineru_4(
            model_base_dir=model_base_dir,
            small_backend=small_backend,
            vlm_engine=vlm_engine,
            vlm_server_url=vlm_server_url,
            vlm_api_key=vlm_api_key,
            vlm_model=vlm_model,
            vlm_max_concurrency=vlm_max_concurrency,
        )
        require_mineru_4()
        self.effort = effort
        self.image_analysis = image_analysis
        self.gpu_memory_utilization = gpu_memory_utilization
        self.rendered_input = rendered_input
        if layout_batch_size <= 0:
            raise ValueError("layout_batch_size must be positive")
        self.layout_batch_size = layout_batch_size
        self._hybrid_model = None
        self._predictor = None

    def _hybrid(self):
        if self._hybrid_model is None:
            from mineru.model.runtime.hybrid import (
                HybridLocalModelContextSingleton,
            )

            self._hybrid_model = HybridLocalModelContextSingleton().get_model()
        return self._hybrid_model

    def _vlm(self):
        if self._predictor is None:
            from mineru.config import config
            from mineru.model.registry import vlm_model_repo
            from mineru.model.vlm.runtime import ModelSingleton
            from mineru.model.vlm.selector import get_vlm_engine

            settings = config.model.vlm.model_copy(deep=True)
            settings.validate_environment()
            if settings.server_url:
                self._predictor = ModelSingleton().get_model(
                    backend="http-client",
                    model_path=None,
                    server_url=settings.server_url,
                    model_name=settings.model or None,
                    server_headers=(
                        {"Authorization": f"Bearer {settings.api_key}"}
                        if settings.api_key
                        else {}
                    ),
                    http_timeout=settings.http_timeout,
                    max_concurrency=settings.max_concurrency,
                )
            else:
                backend = get_vlm_engine(settings.engine, is_async=True)
                model_path = str(vlm_model_repo(settings.engine).local_dir())
                self._predictor = ModelSingleton().get_model(
                    backend=backend,
                    model_path=model_path,
                    server_url=None,
                    max_concurrency=settings.max_concurrency,
                    gpu_memory_utilization=self.gpu_memory_utilization,
                )
        return self._predictor

    def run(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Process one RayOrch batch while preserving one output per window."""

        from docvortex.document.pdf import PDFDocument
        from mineru.backend.analysis.pdf.window import (
            _ProcessingWindow,
            _prepare_pdf_window,
        )
        from mineru.model.runtime.execution import (
            acquire_document,
            local_model_stage,
            release_document,
        )
        from mineru.model.runtime.memory import clean_memory

        outputs: list[dict[str, Any] | None] = [None] * len(records)
        prepared: list[dict[str, Any]] = []
        try:
            if self.rendered_input:
                prepared = self._prepare_rendered_batch(records)
            else:
                for output_index, record in enumerate(records):
                    file_bytes = Path(record["pdf_path"]).read_bytes()
                    document = PDFDocument(file_bytes)
                    if record["native_txt"]:
                        started_at = time.perf_counter()
                        try:
                            from docvortex.analyzers.native import PdfModel
                            from docvortex.document.pdf.visuals import (
                                attach_visual_block_images_from_pdf,
                            )

                            pages = PdfModel().predict(document)
                            attach_visual_block_images_from_pdf(document, pages)
                            outputs[output_index] = _window_output(
                                record,
                                pages,
                                time.perf_counter() - started_at,
                            )
                        finally:
                            document.close()
                        continue

                    hybrid = self._hybrid()
                    acquire_document(hybrid.device)
                    window = _ProcessingWindow(
                        index=int(record["window_index"]),
                        total=int(record["window_count"]),
                        start=int(record["start"]),
                        end=int(record["end"]),
                    )
                    started_at = time.perf_counter()
                    recorder = _StageRecorder()
                    try:
                        with _capture_window_stage_timers(recorder):
                            with local_model_stage(hybrid.device):
                                state = _prepare_pdf_window(
                                    file_bytes,
                                    document,
                                    window,
                                    page_count=int(record["page_count"]),
                                    effort=self.effort,
                                    parse_mode=record["parse_mode"],
                                    hybrid_model=hybrid,
                                )
                    except BaseException:
                        document.close()
                        release_document(hybrid.device, clean_memory)
                        raise
                    prepared.append(
                        {
                            "output_index": output_index,
                            "record": record,
                            "document": document,
                            "state": state,
                            # Keep only time spent executing this window. Using
                            # a start timestamp here and reading it after the
                            # whole mixed batch would double-count wait time.
                            "elapsed_s": time.perf_counter() - started_at,
                            "stage_timings_s": dict(recorder.seconds),
                        }
                    )

            self._infer_and_finish(prepared, outputs)
            if any(output is None for output in outputs):
                raise RuntimeError("MinerU window batch did not produce every output")
            return [output for output in outputs if output is not None]
        finally:
            for item in prepared:
                state = item["state"]
                document = item["document"]
                hybrid = self._hybrid_model
                try:
                    state.close()
                finally:
                    document.close()
                    if hybrid is not None:
                        release_document(hybrid.device, clean_memory)

    def _prepare_rendered_batch(
        self,
        records: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Build MinerU window state and batch Layout across ready lineages."""

        from PIL import Image
        from docvortex.document.pdf import PDFDocument
        import numpy as np
        from mineru.backend.analysis.pdf.layout import (
            _build_vl_style_layout_blocks,
            _collect_table_items,
        )
        from mineru.backend.analysis.pdf.tables import (
            _apply_native_txt_table_priority,
            _apply_table_orientations,
            _split_native_high_table_blocks,
        )
        from mineru.backend.analysis.pdf.window import (
            _ProcessingWindow,
            _WindowInputs,
            _get_window_pdf_pages,
            _log_processing_window,
        )
        from mineru.model.runtime.execution import (
            acquire_document,
            local_model_stage,
            release_document,
        )
        from mineru.model.runtime.memory import clean_memory

        hybrid = self._hybrid()
        prepared: list[dict[str, Any]] = []
        try:
            for output_index, record in enumerate(records):
                if record["native_txt"]:
                    raise ValueError(
                        "rendered V4 analysis only supports model-backed windows"
                    )
                rendered_pages = record.get("rendered_pages")
                if not isinstance(rendered_pages, list):
                    raise ValueError(
                        "MinerU V4 Analyze requires output from RenderWindow"
                    )
                document = PDFDocument(Path(record["pdf_path"]).read_bytes())
                acquire_document(hybrid.device)
                images_list: list[dict[str, Any]] = []
                try:
                    window = _ProcessingWindow(
                        index=int(record["window_index"]),
                        total=int(record["window_count"]),
                        start=int(record["start"]),
                        end=int(record["end"]),
                    )
                    window_pages = _get_window_pdf_pages(document, window)
                    np_images = []
                    for page in rendered_pages:
                        pixels = np.asarray(page["pixels"])
                        if pixels.dtype != np.uint8:
                            raise ValueError("rendered page pixels must use uint8")
                        pil_image = Image.fromarray(pixels)
                        images_list.append(
                            {
                                "img_pil": pil_image,
                                "scale": page.get("scale"),
                            }
                        )
                        np_images.append(pixels)
                    if len(window_pages) != len(images_list):
                        raise ValueError(
                            "rendered window PDF page count does not match image count"
                        )
                    images_pil_list = [
                        image["img_pil"] for image in images_list
                    ]
                    _log_processing_window(
                        window,
                        int(record["page_count"]),
                        len(images_pil_list),
                    )
                    page_text_geometries = (
                        [None] * len(window_pages)
                        if record["parse_mode"] == "txt"
                        and self.effort in {"medium", "high", "xhigh"}
                        else None
                    )
                    page_vector_geometries = (
                        [None] * len(window_pages)
                        if page_text_geometries is not None
                        else None
                    )
                    prepared.append(
                        {
                            "output_index": output_index,
                            "record": record,
                            "document": document,
                            "window": window,
                            "window_pages": window_pages,
                            "images_list": images_list,
                            "images_pil_list": images_pil_list,
                            "np_images": np_images,
                            "page_text_geometries": page_text_geometries,
                            "page_vector_geometries": page_vector_geometries,
                            "stage_timings_s": dict(
                                record.get("stage_timings_s", {})
                            ),
                        }
                    )
                except BaseException:
                    for image in images_list:
                        image["img_pil"].close()
                    document.close()
                    release_document(hybrid.device, clean_memory)
                    raise

            all_images = [
                image
                for item in prepared
                for image in item["images_pil_list"]
            ]
            layout_recorder = _StageRecorder()
            with layout_recorder.timer("pdf.layout"):
                with local_model_stage(hybrid.device):
                    all_layouts = hybrid.layout_model.batch_predict(
                        all_images,
                        batch_size=self.layout_batch_size,
                    )
            page_counts = [
                len(item["images_pil_list"]) for item in prepared
            ]
            if len(all_layouts) != sum(page_counts):
                raise ValueError(
                    "MinerU layout output count does not match rendered pages"
                )
            _allocate_stage_timings(
                prepared,
                dict(layout_recorder.seconds),
                page_counts=page_counts,
            )

            cursor = 0
            for item, count in zip(prepared, page_counts, strict=True):
                images_layout_res = all_layouts[cursor : cursor + count]
                cursor += count
                item_recorder = _StageRecorder()
                table_items = _collect_table_items(
                    images_layout_res,
                    item["np_images"],
                )
                if table_items and self.effort in {"flash", "medium", "high"}:
                    with item_recorder.timer("pdf.table_orientation"):
                        with local_model_stage(hybrid.device):
                            _apply_table_orientations(
                                table_items,
                                item["record"]["parse_mode"],
                                item["window_pages"],
                                item["images_list"],
                                hybrid,
                                item["page_text_geometries"],
                            )
                vl_style_layout_blocks = _build_vl_style_layout_blocks(
                    images_layout_res,
                    item["images_pil_list"],
                )
                if (
                    item["record"]["parse_mode"] == "txt"
                    and self.effort in {"medium", "high"}
                ):
                    with item_recorder.timer("pdf.native_tables"):
                        _apply_native_txt_table_priority(
                            vl_style_layout_blocks,
                            images_layout_res,
                            item["window_pages"],
                            item["images_list"],
                            effort=self.effort,
                            page_text_geometries=item[
                                "page_text_geometries"
                            ],
                            page_vector_geometries=item[
                                "page_vector_geometries"
                            ],
                        )
                high_vlm_blocks = vl_style_layout_blocks
                accepted_native_tables = []
                if (
                    item["record"]["parse_mode"] == "txt"
                    and self.effort == "high"
                ):
                    (
                        high_vlm_blocks,
                        accepted_native_tables,
                    ) = _split_native_high_table_blocks(
                        vl_style_layout_blocks
                    )
                item["state"] = _WindowInputs(
                    item.pop("window"),
                    item.pop("images_list"),
                    item.pop("window_pages"),
                    item.pop("images_pil_list"),
                    item.pop("np_images"),
                    images_layout_res,
                    vl_style_layout_blocks,
                    high_vlm_blocks,
                    accepted_native_tables,
                    item.pop("page_text_geometries"),
                    item.pop("page_vector_geometries"),
                )
                _merge_stage_timings(
                    item["stage_timings_s"],
                    dict(item_recorder.seconds),
                )
                # Keep compatibility with the existing document-level infer
                # cost while the detailed profile remains separately additive.
                item["elapsed_s"] = sum(item["stage_timings_s"].values())
            return prepared
        except BaseException:
            for item in prepared:
                state = item.get("state")
                if state is not None:
                    state.close()
                else:
                    for image in item.get("images_list", []):
                        image["img_pil"].close()
                item["document"].close()
                release_document(hybrid.device, clean_memory)
            raise

    def _infer_and_finish(
        self,
        prepared: list[dict[str, Any]],
        outputs: list[dict[str, Any] | None],
    ) -> None:
        from mineru.backend.analysis.pdf.constants import NOT_EXTRACT_TYPES
        from mineru.backend.analysis.pdf.window import _finish_pdf_window
        from mineru.model.runtime.execution import local_model_stage

        hybrid = self._hybrid_model
        if hybrid is None:
            return

        if self.effort == "medium":
            self._finish_medium_batch(prepared, outputs, hybrid)
            return

        if self.effort == "flash":
            for item in prepared:
                state = item["state"]
                recorder = _StageRecorder()
                finish_started_at = time.perf_counter()
                with _capture_window_stage_timers(recorder):
                    with local_model_stage(hybrid.device):
                        pages = _finish_pdf_window(
                            state,
                            state.vl_style_layout_blocks,
                            effort=self.effort,
                            parse_mode=item["record"]["parse_mode"],
                            hybrid_model=hybrid,
                        )
                _merge_stage_timings(
                    item.setdefault("stage_timings_s", {}),
                    dict(recorder.seconds),
                )
                outputs[item["output_index"]] = _window_output(
                    item["record"],
                    pages,
                    item["elapsed_s"] + time.perf_counter() - finish_started_at,
                    item["stage_timings_s"],
                )
            return

        predictor = self._vlm()
        for mode in ("txt", "ocr"):
            selected = [
                item for item in prepared if item["record"]["parse_mode"] == mode
            ]
            if not selected:
                continue
            images = [
                image
                for item in selected
                for image in item["state"].images_pil_list
            ]
            options: dict[str, Any] = {
                "images": images,
                "image_analysis": (
                    self.image_analysis if self.effort == "xhigh" else False
                ),
            }
            if self.effort == "high":
                options["blocks_list"] = [
                    blocks
                    for item in selected
                    for blocks in item["state"].high_vlm_blocks
                ]
            if mode == "txt":
                options["not_extract_list"] = NOT_EXTRACT_TYPES
            infer_started_at = time.perf_counter()
            if self.effort == "high":
                inferred = predictor.batch_extract_with_layout(**options)
            else:
                inferred = predictor.batch_two_step_extract(**options)
            infer_elapsed_s = time.perf_counter() - infer_started_at
            inferred_pages = sum(
                len(item["state"].images_pil_list) for item in selected
            )
            cursor = 0
            _allocate_stage_timings(
                selected,
                {"pdf.vlm": infer_elapsed_s},
                page_counts=[
                    len(item["state"].images_pil_list) for item in selected
                ],
            )
            if self.effort == "xhigh":
                self._finish_xhigh_batch(
                    selected,
                    inferred,
                    outputs,
                    hybrid,
                    infer_elapsed_s,
                )
                continue
            for item in selected:
                state = item["state"]
                count = len(state.images_pil_list)
                window_result = inferred[cursor : cursor + count]
                cursor += count
                recorder = _StageRecorder()
                finish_started_at = time.perf_counter()
                with _capture_window_stage_timers(recorder):
                    with local_model_stage(hybrid.device):
                        pages = _finish_pdf_window(
                            state,
                            window_result,
                            effort=self.effort,
                            parse_mode=mode,
                            hybrid_model=hybrid,
                        )
                _merge_stage_timings(
                    item.setdefault("stage_timings_s", {}),
                    dict(recorder.seconds),
                )
                outputs[item["output_index"]] = _window_output(
                    item["record"],
                    pages,
                    item["elapsed_s"]
                    + (
                        infer_elapsed_s * count / inferred_pages
                        if inferred_pages
                        else 0.0
                    )
                    + time.perf_counter()
                    - finish_started_at,
                    item["stage_timings_s"],
                )
            if cursor != len(inferred):
                raise ValueError("MinerU VLM output count does not match input pages")

    def _finish_xhigh_batch(
        self,
        selected: list[dict[str, Any]],
        inferred: list[Any],
        outputs: list[dict[str, Any] | None],
        hybrid: Any,
        infer_elapsed_s: float,
    ) -> None:
        """Finish Advanced pages together after one shared two-step VLM call.

        The VLM remains one actor-local predictor.  Only its already completed
        page results and MinerU's local MFR/OCR/fill work are flattened across
        ready windows, then sliced back to their original lineage.
        """

        from docvortex.assets import image_size as normalize_page_size
        from docvortex.document.pdf.visuals import attach_visual_block_images
        from mineru.backend.analysis.pdf.layout import (
            _convert_vlm_results_to_model_list,
            _normalize_xhigh_vlm_blocks,
        )
        from mineru.backend.analysis.pdf.normalization import (
            _apply_layout_title_split,
        )
        from mineru.backend.analysis.pdf.visual_containers import (
            supplement_missing_image_block_containers,
        )
        from mineru.backend.analysis.pdf.window import _process_text_and_formulas
        from mineru.model.runtime.execution import local_model_stage

        states = [item["state"] for item in selected]
        page_counts = [len(state.images_list) for state in states]
        total_pages = sum(page_counts)
        if len(inferred) != total_pages:
            raise ValueError(
                "MinerU Advanced VLM output count does not match input pages"
            )

        images_list = [image for state in states for image in state.images_list]
        images_pil_list = [
            image for state in states for image in state.images_pil_list
        ]
        window_pages = [page for state in states for page in state.window_pages]
        np_images = [image for state in states for image in state.np_images]
        images_layout_res = [
            layout for state in states for layout in state.images_layout_res
        ]
        local_layout_blocks = [
            blocks for state in states for blocks in state.vl_style_layout_blocks
        ]
        mode = selected[0]["record"]["parse_mode"]
        page_text_geometries = (
            [
                geometry
                for state in states
                for geometry in (state.page_text_geometries or [])
            ]
            if mode == "txt"
            else None
        )
        page_vector_geometries = (
            [
                geometry
                for state in states
                for geometry in (state.page_vector_geometries or [])
            ]
            if mode == "txt"
            else None
        )
        if not (
            len(images_list)
            == len(images_pil_list)
            == len(window_pages)
            == len(np_images)
            == len(images_layout_res)
            == len(local_layout_blocks)
            == total_pages
        ):
            raise ValueError(
                "MinerU Advanced batch has inconsistent page counts"
            )
        if mode == "txt" and (
            len(page_text_geometries or []) != total_pages
            or len(page_vector_geometries or []) != total_pages
        ):
            raise ValueError(
                "MinerU Advanced TXT batch has inconsistent geometry counts"
            )

        recorder = _StageRecorder()
        finish_started_at = time.perf_counter()
        model_list = _convert_vlm_results_to_model_list(inferred)
        _normalize_xhigh_vlm_blocks(model_list)
        _apply_layout_title_split(
            model_list,
            images_layout_res,
            [normalize_page_size(image) for image in images_pil_list],
        )
        with _capture_window_stage_timers(recorder):
            with local_model_stage(hybrid.device):
                model_list = _process_text_and_formulas(
                    images_list,
                    window_pages,
                    model_list,
                    mode,
                    "xhigh",
                    hybrid,
                    images_layout_res,
                    page_text_geometries,
                    page_vector_geometries=page_vector_geometries,
                    np_images=np_images,
                )
        supplement_missing_image_block_containers(
            model_list,
            local_layout_blocks,
        )
        finish_elapsed_s = time.perf_counter() - finish_started_at
        _allocate_stage_timings(
            selected,
            dict(recorder.seconds),
            page_counts=page_counts,
        )

        cursor = 0
        for item, count in zip(selected, page_counts, strict=True):
            state = item["state"]
            window_result = model_list[cursor : cursor + count]
            cursor += count
            asset_recorder = _StageRecorder()
            asset_started_at = time.perf_counter()
            with asset_recorder.timer("pdf.image_assets"):
                attach_visual_block_images(
                    window_result,
                    state.images_list,
                    page_start_index=state.window.start,
                )
            _merge_stage_timings(
                item.setdefault("stage_timings_s", {}),
                dict(asset_recorder.seconds),
            )
            page_share = count / total_pages if total_pages else 0.0
            outputs[item["output_index"]] = _window_output(
                item["record"],
                window_result,
                item["elapsed_s"]
                + infer_elapsed_s * page_share
                + finish_elapsed_s * page_share
                + time.perf_counter()
                - asset_started_at,
                item["stage_timings_s"],
            )
        if cursor != len(model_list):
            raise ValueError(
                "MinerU Advanced output count does not match restored windows"
            )

    def _finish_medium_batch(
        self,
        prepared: list[dict[str, Any]],
        outputs: list[dict[str, Any] | None],
        hybrid: Any,
    ) -> None:
        """Batch Basic's local model stages across all ready windows.

        The live PDF, PIL and model objects stay inside one Analyze actor. Only
        the per-page arrays are flattened, passed through MinerU's existing
        batched formula/table/OCR implementation, and then sliced back into
        their original windows before visual assets are attached.
        """

        from docvortex.document.pdf.visuals import attach_visual_block_images
        from mineru.backend.analysis.pdf.ocr import _apply_seal_ocr
        from mineru.backend.analysis.pdf.window import _process_text_and_formulas
        from mineru.model.runtime.execution import local_model_stage

        for mode in ("txt", "ocr"):
            selected = [
                item for item in prepared if item["record"]["parse_mode"] == mode
            ]
            if not selected:
                continue

            states = [item["state"] for item in selected]
            page_counts = [len(state.images_list) for state in states]
            total_pages = sum(page_counts)
            images_list = [image for state in states for image in state.images_list]
            window_pages = [page for state in states for page in state.window_pages]
            model_list = [
                blocks for state in states for blocks in state.vl_style_layout_blocks
            ]
            np_images = [image for state in states for image in state.np_images]
            images_layout_res = [
                layout for state in states for layout in state.images_layout_res
            ]
            page_text_geometries = (
                [
                    geometry
                    for state in states
                    for geometry in (state.page_text_geometries or [])
                ]
                if mode == "txt"
                else None
            )
            page_vector_geometries = (
                [
                    geometry
                    for state in states
                    for geometry in (state.page_vector_geometries or [])
                ]
                if mode == "txt"
                else None
            )
            if not (
                len(images_list)
                == len(window_pages)
                == len(model_list)
                == len(np_images)
                == len(images_layout_res)
                == total_pages
            ):
                raise ValueError("MinerU medium batch has inconsistent page counts")
            if mode == "txt" and (
                len(page_text_geometries or []) != total_pages
                or len(page_vector_geometries or []) != total_pages
            ):
                raise ValueError(
                    "MinerU medium TXT batch has inconsistent geometry counts"
                )

            recorder = _StageRecorder()
            infer_started_at = time.perf_counter()
            with _capture_window_stage_timers(recorder):
                with local_model_stage(hybrid.device):
                    inferred = _process_text_and_formulas(
                        images_list,
                        window_pages,
                        model_list,
                        mode,
                        "medium",
                        hybrid,
                        images_layout_res,
                        page_text_geometries,
                        page_vector_geometries=page_vector_geometries,
                        np_images=np_images,
                    )
                    with recorder.timer("pdf.seals"):
                        _apply_seal_ocr(hybrid, inferred, np_images)
            infer_elapsed_s = time.perf_counter() - infer_started_at
            if len(inferred) != total_pages:
                raise ValueError(
                    "MinerU medium output count does not match input pages"
                )
            _allocate_stage_timings(
                selected,
                dict(recorder.seconds),
                page_counts=page_counts,
            )

            cursor = 0
            for item, count in zip(selected, page_counts, strict=True):
                state = item["state"]
                window_result = inferred[cursor : cursor + count]
                cursor += count
                finish_recorder = _StageRecorder()
                finish_started_at = time.perf_counter()
                with finish_recorder.timer("pdf.image_assets"):
                    attach_visual_block_images(
                        window_result,
                        state.images_list,
                        page_start_index=state.window.start,
                    )
                _merge_stage_timings(
                    item.setdefault("stage_timings_s", {}),
                    dict(finish_recorder.seconds),
                )
                outputs[item["output_index"]] = _window_output(
                    item["record"],
                    window_result,
                    item["elapsed_s"]
                    + (
                        infer_elapsed_s * count / total_pages
                        if total_pages
                        else 0.0
                    )
                    + time.perf_counter()
                    - finish_started_at,
                    item["stage_timings_s"],
                )
            if cursor != len(inferred):
                raise ValueError(
                    "MinerU medium output count does not match restored windows"
                )

    def close(self) -> None:
        """Release MinerU's process-local VLM cache when the actor exits."""

        if self._predictor is not None:
            from mineru.model.vlm.runtime import shutdown_cached_models

            shutdown_cached_models()
            self._predictor = None
        self._hybrid_model = None


def _window_output(
    record: dict[str, Any],
    pages: list[list[dict[str, Any]]],
    elapsed: float,
    stage_timings_s: dict[str, float] | None = None,
) -> dict[str, Any]:
    output = {
        "pdf_path": record["pdf_path"],
        "start": record["start"],
        "end": record["end"],
        "parse_mode": record["parse_mode"],
        "pages": pages,
        "elapsed_s": elapsed,
    }
    if stage_timings_s:
        output["stage_timings_s"] = dict(stage_timings_s)
    return output


def save_mineru_4_documents(
    grouped_windows: list[list[dict[str, Any]]],
    pdf_paths: list[str],
    *,
    output_dir: str,
    tier: str,
    effort: MinerU4Effort,
) -> list[str]:
    """Build and save MinerU 4's official ModelJson and MiddleJson outputs."""

    require_mineru_4()
    from docvortex.document.pdf import PDFDocument
    from docvortex.document.pdf.layout import (
        attach_layout_image_rotations,
        extract_layout_geometry,
    )
    from mineru.backend.analyze import _build_model_json
    from mineru.backend.analysis.contracts import AnalysisResult
    from mineru.backend.analysis.pdf.normalization import (
        _normalize_pdf_model_list,
    )
    from mineru.backend.postprocess.document import model_json_to_middle_json
    from mineru.config import config
    from mineru.integrations.docvortex import read_source_properties
    from mineru.parser.base import ParseResult
    from mineru.parser.writer import FileBasedDataWriter

    outputs = []
    for windows, raw_path in zip(grouped_windows, pdf_paths, strict=True):
        assemble_started_at = time.perf_counter()
        if not windows:
            raise ValueError(f"MinerU produced no windows for {raw_path}")
        windows = sorted(windows, key=lambda item: item["start"])
        pages = [page for window in windows for page in window["pages"]]
        parse_modes = {window["parse_mode"] for window in windows}
        if len(parse_modes) != 1:
            raise ValueError("all windows of one PDF must use the same parse mode")

        angles = {
            id(block): block.get("angle", 0)
            for page in pages
            for block in page
        }
        _normalize_pdf_model_list(pages)
        path = Path(raw_path).expanduser().resolve()
        file_bytes = path.read_bytes()
        with PDFDocument(file_bytes) as document:
            geometry, _diagnostics = extract_layout_geometry(document, None)
        rotation_pages = [
            [{**block, "angle": angles.get(id(block), 0)} for block in page]
            for page in pages
        ]
        attach_layout_image_rotations(geometry, rotation_pages, None)
        analysis = AnalysisResult(
            pages,
            effort,
            parse_modes.pop(),
            sum(float(window.get("elapsed_s", 0.0)) for window in windows),
            geometry,
        )
        source_properties = read_source_properties(file_bytes, "pdf")
        model_json = _build_model_json(
            analysis,
            "pdf",
            None,
            source_properties,
        )
        middle_json = model_json_to_middle_json(
            model_json,
            llm_aided_config=config.llm_aided,
        )
        result = ParseResult(middle_json=middle_json, _model_output=model_json)
        result_dir = Path(output_dir) / path.stem / tier
        result.save(FileBasedDataWriter(str(result_dir)))
        stage_timings_s: dict[str, float] = {}
        for window in windows:
            _merge_stage_timings(
                stage_timings_s,
                window.get("stage_timings_s"),
            )
        stage_timings_s["pdf.assemble"] = (
            stage_timings_s.get("pdf.assemble", 0.0)
            + time.perf_counter()
            - assemble_started_at
        )
        manifest = {
            "pipeline": f"v4-{tier}",
            "source": str(path),
            "pages": len(pages),
            "parse_mode": analysis.parse_mode,
            "stage_profile": _profile_summary(stage_timings_s),
        }
        (result_dir / "flash_mineru.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        outputs.append("markdown.md")
    return outputs


__all__ = [
    "MinerU4RenderWindow",
    "MinerU4WindowAnalyzer",
    "configure_mineru_4",
    "prepare_mineru_4_models",
    "require_mineru_4",
    "save_mineru_4_documents",
    "split_pdf_windows",
]
