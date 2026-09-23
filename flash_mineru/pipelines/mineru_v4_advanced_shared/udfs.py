"""UDFs for MinerU 4 Advanced with one Actor Pool shared by model stages."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from flash_mineru.pipelines._shared.mineru_v4 import (
    MinerU4RenderWindow,
    MinerU4WindowAnalyzer,
    _StageRecorder,
    _allocate_stage_timings,
    _capture_window_stage_timers,
    _merge_stage_timings,
    _window_output,
    enable_local_vllm_compatibility,
    save_mineru_4_documents,
    split_pdf_windows,
)


class MinerU4AdvancedSharedSplitPdf:
    """Split each PDF into ordered windows that can move independently."""

    def __init__(
        self,
        window_size: int = 1,
        parse_mode: str = "auto",
    ) -> None:
        self.window_size = window_size
        self.parse_mode = parse_mode

    def run(self, pdf_paths: list[str]) -> list[list[dict[str, Any]]]:
        return split_pdf_windows(
            pdf_paths,
            effort="xhigh",
            parse_mode=self.parse_mode,
            window_size=self.window_size,
        )


class MinerU4AdvancedSharedRenderWindow(MinerU4RenderWindow):
    """Render one ordered PDF window on a CPU actor."""


class MinerU4AdvancedSharedModelStack(MinerU4WindowAnalyzer):
    """Run two DAG stages on one shared pool of complete model stacks.

    The actor owns stable Layout, Two-Step VLM, MFR, and OCR models. Every
    request-specific value is carried by the returned record because RayOrch
    intentionally does not pin one lineage to one physical replica.
    """

    def __init__(
        self,
        *,
        vlm_server_url: str = "",
        vlm_engine: str = "vllm",
        **kwargs: Any,
    ) -> None:
        if vlm_server_url:
            raise ValueError(
                "v4-advanced-shared requires an actor-local VLM; "
                "vlm_server_url must be empty"
            )
        if vlm_engine != "vllm":
            raise ValueError(
                "v4-advanced-shared currently requires vlm_engine='vllm'"
            )
        enable_local_vllm_compatibility()
        super().__init__(
            effort="xhigh",
            vlm_server_url="",
            vlm_engine=vlm_engine,
            **kwargs,
        )

    def run(
        self,
        records: list[dict[str, Any]],
        rendered_records: list[dict[str, Any]] | None = None,
        *,
        stage: str,
    ) -> list[dict[str, Any]]:
        if stage == "infer":
            if rendered_records is not None:
                raise ValueError("infer accepts only rendered records")
            return self._run_infer(records)
        if stage == "finish":
            if rendered_records is None:
                raise ValueError("finish requires the original rendered records")
            return self._run_finish(records, rendered_records)
        raise ValueError(f"unknown v4-advanced-shared stage: {stage!r}")

    @staticmethod
    def _pil_pages(record: dict[str, Any]) -> tuple[list[dict[str, Any]], list[Any]]:
        from PIL import Image
        import numpy as np

        rendered_pages = record.get("rendered_pages")
        if not isinstance(rendered_pages, list):
            raise ValueError("shared Advanced stage requires RenderWindow output")
        images_list: list[dict[str, Any]] = []
        for page in rendered_pages:
            pixels = np.asarray(page["pixels"])
            if pixels.dtype != np.uint8:
                raise ValueError("rendered page pixels must use uint8")
            images_list.append(
                {
                    "img_pil": Image.fromarray(pixels),
                    "scale": page.get("scale"),
                }
            )
        return images_list, [item["img_pil"] for item in images_list]

    def _run_infer(
        self, records: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Batch local Layout and Two-Step VLM, then emit serializable state."""

        from mineru.backend.analysis.pdf.constants import NOT_EXTRACT_TYPES
        from mineru.backend.analysis.pdf.layout import (
            _build_vl_style_layout_blocks,
        )
        from mineru.model.runtime.execution import local_model_stage

        hybrid = self._hybrid()
        predictor = self._vlm()
        prepared: list[dict[str, Any]] = []
        try:
            for output_index, record in enumerate(records):
                if record["native_txt"]:
                    raise ValueError(
                        "shared Advanced stages only support model-backed windows"
                    )
                images_list, images_pil_list = self._pil_pages(record)
                prepared.append(
                    {
                        "output_index": output_index,
                        "record": record,
                        "images_list": images_list,
                        "images_pil_list": images_pil_list,
                        "stage_timings_s": dict(
                            record.get("stage_timings_s", {})
                        ),
                    }
                )

            all_images = [
                image for item in prepared for image in item["images_pil_list"]
            ]
            layout_recorder = _StageRecorder()
            with layout_recorder.timer("pdf.layout"):
                with local_model_stage(hybrid.device):
                    all_layouts = hybrid.layout_model.batch_predict(
                        all_images,
                        batch_size=self.layout_batch_size,
                    )
            page_counts = [len(item["images_pil_list"]) for item in prepared]
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
                layouts = all_layouts[cursor : cursor + count]
                cursor += count
                item["images_layout_res"] = layouts
                item["vl_style_layout_blocks"] = (
                    _build_vl_style_layout_blocks(
                        layouts, item["images_pil_list"]
                    )
                )

            outputs: list[dict[str, Any] | None] = [None] * len(records)
            for mode in ("txt", "ocr"):
                selected = [
                    item
                    for item in prepared
                    if item["record"]["parse_mode"] == mode
                ]
                if not selected:
                    continue
                images = [
                    image
                    for item in selected
                    for image in item["images_pil_list"]
                ]
                selected_counts = [
                    len(item["images_pil_list"]) for item in selected
                ]
                started_at = time.perf_counter()
                inferred = predictor.batch_two_step_extract(
                    images=images,
                    not_extract_list=(
                        NOT_EXTRACT_TYPES if mode == "txt" else None
                    ),
                    image_analysis=self.image_analysis,
                )
                elapsed_s = time.perf_counter() - started_at
                if len(inferred) != sum(selected_counts):
                    raise ValueError(
                        "MinerU Advanced VLM output count does not match input pages"
                    )
                _allocate_stage_timings(
                    selected,
                    {"pdf.vlm": elapsed_s},
                    page_counts=selected_counts,
                )
                cursor = 0
                for item, count in zip(
                    selected, selected_counts, strict=True
                ):
                    output: dict[str, Any] = {}
                    output["images_layout_res"] = item["images_layout_res"]
                    output["vl_style_layout_blocks"] = item[
                        "vl_style_layout_blocks"
                    ]
                    output["vlm_results"] = [
                        [dict(block) for block in page]
                        for page in inferred[cursor : cursor + count]
                    ]
                    cursor += count
                    output["stage_timings_s"] = item["stage_timings_s"]
                    outputs[item["output_index"]] = output
            if any(output is None for output in outputs):
                raise RuntimeError("MinerU infer stage did not produce every output")
            return [output for output in outputs if output is not None]
        finally:
            for item in prepared:
                for image in item["images_list"]:
                    image["img_pil"].close()

    def _run_finish(
        self,
        records: list[dict[str, Any]],
        rendered_records: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Batch MFR/OCR/fill, restore each window, and release resources."""

        from docvortex.assets import image_size as normalize_page_size
        from docvortex.document.pdf import PDFDocument
        from docvortex.document.pdf.visuals import attach_visual_block_images
        import numpy as np
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
        from mineru.backend.analysis.pdf.window import (
            _ProcessingWindow,
            _get_window_pdf_pages,
            _process_text_and_formulas,
        )
        from mineru.model.runtime.execution import (
            acquire_document,
            local_model_stage,
            release_document,
        )
        from mineru.model.runtime.memory import clean_memory

        if len(records) != len(rendered_records):
            raise ValueError("inferred and rendered batch sizes do not match")
        hybrid = self._hybrid()
        outputs: list[dict[str, Any] | None] = [None] * len(records)
        prepared: list[dict[str, Any]] = []
        try:
            for output_index, (inferred_record, rendered_record) in enumerate(
                zip(records, rendered_records, strict=True)
            ):
                images_list, images_pil_list = self._pil_pages(rendered_record)
                document = PDFDocument(
                    Path(rendered_record["pdf_path"]).read_bytes()
                )
                acquire_document(hybrid.device)
                try:
                    window = _ProcessingWindow(
                        index=int(rendered_record["window_index"]),
                        total=int(rendered_record["window_count"]),
                        start=int(rendered_record["start"]),
                        end=int(rendered_record["end"]),
                    )
                    window_pages = _get_window_pdf_pages(document, window)
                    if len(window_pages) != len(images_pil_list):
                        raise ValueError(
                            "shared Advanced window page count does not match images"
                        )
                    layouts = inferred_record.get("images_layout_res")
                    local_blocks = inferred_record.get("vl_style_layout_blocks")
                    inferred = inferred_record.get("vlm_results")
                    if not all(isinstance(value, list) for value in (
                        layouts, local_blocks, inferred
                    )):
                        raise ValueError(
                            "shared Advanced finish requires Layout and VLM outputs"
                        )
                    model_list = _convert_vlm_results_to_model_list(inferred)
                    _normalize_xhigh_vlm_blocks(model_list)
                    _apply_layout_title_split(
                        model_list,
                        layouts,
                        [normalize_page_size(image) for image in images_pil_list],
                    )
                    prepared.append(
                        {
                            "output_index": output_index,
                            "record": rendered_record,
                            "document": document,
                            "window": window,
                            "window_pages": window_pages,
                            "images_list": images_list,
                            "images_pil_list": images_pil_list,
                            "np_images": [
                                np.asarray(page["pixels"])
                                for page in rendered_record["rendered_pages"]
                            ],
                            "images_layout_res": layouts,
                            "local_layout_blocks": local_blocks,
                            "model_list": model_list,
                            "page_text_geometries": (
                                [None] * len(window_pages)
                                if rendered_record["parse_mode"] == "txt"
                                else None
                            ),
                            "page_vector_geometries": (
                                [None] * len(window_pages)
                                if rendered_record["parse_mode"] == "txt"
                                else None
                            ),
                            "stage_timings_s": dict(
                                inferred_record.get("stage_timings_s", {})
                            ),
                        }
                    )
                except BaseException:
                    for image in images_list:
                        image["img_pil"].close()
                    document.close()
                    release_document(hybrid.device, clean_memory)
                    raise

            for mode in ("txt", "ocr"):
                selected = [
                    item
                    for item in prepared
                    if item["record"]["parse_mode"] == mode
                ]
                if not selected:
                    continue
                page_counts = [len(item["images_list"]) for item in selected]
                total_pages = sum(page_counts)
                images_list = [
                    image for item in selected for image in item["images_list"]
                ]
                images_pil_list = [
                    image
                    for item in selected
                    for image in item["images_pil_list"]
                ]
                window_pages = [
                    page for item in selected for page in item["window_pages"]
                ]
                model_list = [
                    page for item in selected for page in item["model_list"]
                ]
                np_images = [
                    image for item in selected for image in item["np_images"]
                ]
                layouts = [
                    page
                    for item in selected
                    for page in item["images_layout_res"]
                ]
                local_blocks = [
                    page
                    for item in selected
                    for page in item["local_layout_blocks"]
                ]
                text_geometries = (
                    [
                        value
                        for item in selected
                        for value in item["page_text_geometries"]
                    ]
                    if mode == "txt"
                    else None
                )
                vector_geometries = (
                    [
                        value
                        for item in selected
                        for value in item["page_vector_geometries"]
                    ]
                    if mode == "txt"
                    else None
                )
                if not (
                    len(images_list)
                    == len(images_pil_list)
                    == len(window_pages)
                    == len(model_list)
                    == len(np_images)
                    == len(layouts)
                    == len(local_blocks)
                    == total_pages
                ):
                    raise ValueError(
                        "shared Advanced finish has inconsistent page counts"
                    )

                prior_elapsed_by_item = [
                    sum(item["stage_timings_s"].values()) for item in selected
                ]
                recorder = _StageRecorder()
                finish_started_at = time.perf_counter()
                with _capture_window_stage_timers(recorder):
                    with local_model_stage(hybrid.device):
                        model_list = _process_text_and_formulas(
                            images_list,
                            window_pages,
                            model_list,
                            mode,
                            "xhigh",
                            hybrid,
                            layouts,
                            text_geometries,
                            page_vector_geometries=vector_geometries,
                            np_images=np_images,
                        )
                supplement_missing_image_block_containers(
                    model_list,
                    local_blocks,
                )
                finish_elapsed_s = time.perf_counter() - finish_started_at
                _allocate_stage_timings(
                    selected,
                    dict(recorder.seconds),
                    page_counts=page_counts,
                )

                cursor = 0
                for item, count, prior_elapsed_s in zip(
                    selected, page_counts, prior_elapsed_by_item, strict=True
                ):
                    window_result = model_list[cursor : cursor + count]
                    cursor += count
                    asset_recorder = _StageRecorder()
                    asset_started_at = time.perf_counter()
                    with asset_recorder.timer("pdf.image_assets"):
                        attach_visual_block_images(
                            window_result,
                            item["images_list"],
                            page_start_index=item["window"].start,
                        )
                    _merge_stage_timings(
                        item["stage_timings_s"],
                        dict(asset_recorder.seconds),
                    )
                    page_share = count / total_pages if total_pages else 0.0
                    elapsed_s = (
                        prior_elapsed_s
                        + finish_elapsed_s * page_share
                        + time.perf_counter()
                        - asset_started_at
                    )
                    outputs[item["output_index"]] = _window_output(
                        item["record"],
                        window_result,
                        elapsed_s,
                        item["stage_timings_s"],
                    )
                if cursor != len(model_list):
                    raise ValueError(
                        "shared Advanced output count does not match windows"
                    )
            if any(output is None for output in outputs):
                raise RuntimeError("MinerU finish stage did not produce every output")
            return [output for output in outputs if output is not None]
        finally:
            for item in prepared:
                for image in item["images_list"]:
                    image["img_pil"].close()
                item["document"].close()
                release_document(hybrid.device, clean_memory)


class MinerU4AdvancedSharedAssembleDocument:
    """Restore document order and save official Advanced-tier outputs."""

    def __init__(self, output_dir: str) -> None:
        self.output_dir = output_dir

    def run(
        self,
        grouped_windows: list[list[dict[str, Any]]],
        pdf_paths: list[str],
    ) -> list[str]:
        return save_mineru_4_documents(
            grouped_windows,
            pdf_paths,
            output_dir=self.output_dir,
            tier="advanced",
            effort="xhigh",
        )


__all__ = [
    "MinerU4AdvancedSharedAssembleDocument",
    "MinerU4AdvancedSharedModelStack",
    "MinerU4AdvancedSharedRenderWindow",
    "MinerU4AdvancedSharedSplitPdf",
]
