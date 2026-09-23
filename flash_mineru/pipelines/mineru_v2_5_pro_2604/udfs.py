"""Page-grained UDFs for MinerU 2.5 Pro 2604."""

from __future__ import annotations

import json
import os
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any


def _require_mineru_3() -> None:
    """Reject schema-incompatible MinerU releases with an actionable error."""

    try:
        installed = version("mineru")
    except PackageNotFoundError as exc:
        raise ImportError(
            "The v2.5 Pro pipelines require MinerU 3.x in the actor "
            "environment. Install `mineru[vlm]>=3.1,<4`."
        ) from exc
    if int(installed.split(".", 1)[0]) != 3:
        raise ImportError(
            f"The v2.5 Pro pipelines require MinerU 3.x, found {installed}."
        )


class MinerU25Pro2604PdfToPages:
    """Render each PDF into ordered page records."""

    def __init__(
        self,
        dpi: int = 200,
        start_page_id: int = 0,
        end_page_id: int | None = None,
    ) -> None:
        self.dpi = dpi
        self.start_page_id = start_page_id
        self.end_page_id = end_page_id

    def run(self, pdf_paths: list[str]) -> list[list[dict[str, Any]]]:
        from flash_mineru.mineru_core.dispatch_mineru_class import (
            convert_pdf_bytes_to_bytes_by_pypdfium2,
        )
        from flash_mineru.mineru_core.utils.enum_class import ImageType
        from flash_mineru.mineru_core.utils.pdf_image_tools import (
            load_images_from_pdf,
        )

        groups: list[list[dict[str, Any]]] = []
        for raw_path in pdf_paths:
            path = str(Path(raw_path))
            pdf_bytes = Path(path).read_bytes()
            rewritten, _ = convert_pdf_bytes_to_bytes_by_pypdfium2(
                pdf_bytes,
                self.start_page_id,
                self.end_page_id,
            )
            images, pdf_doc = load_images_from_pdf(
                rewritten,
                dpi=self.dpi,
                image_type=ImageType.PIL,
            )
            try:
                pages = []
                for offset, image in enumerate(images):
                    if image is None:
                        continue
                    width, height = map(int, pdf_doc[offset].get_size())
                    pages.append(
                        {
                            **image,
                            "pdf_path": path,
                            "page_id": self.start_page_id + offset,
                            "page_width": width,
                            "page_height": height,
                            "pdf_len": len(images),
                        }
                    )
                groups.append(pages)
            finally:
                pdf_doc.close()
        return groups


class MinerU25Pro2604VlmPage:
    """Run Pro 2604 over cross-document page batches."""

    def __init__(
        self,
        model: str,
        gpu_memory_utilization: float = 0.9,
        image_analysis: bool = True,
    ) -> None:
        _require_mineru_3()
        from mineru_vl_utils import MinerUClient, MinerULogitsProcessor
        from vllm import LLM

        self.image_analysis = image_analysis
        self.llm = LLM(
            model=model,
            gpu_memory_utilization=gpu_memory_utilization,
            logits_processors=[MinerULogitsProcessor],
        )
        self.client = MinerUClient(
            backend="vllm-engine",
            vllm_llm=self.llm,
            enable_table_formula_eq_wrap=True,
            image_analysis=image_analysis,
            # A RayOrch OCR batch may contain pages from unrelated documents.
            # Cross-page table merging must therefore happen after lineage has
            # reconstructed each document, never inside this mixed page batch.
            enable_cross_page_table_merge=False,
        )

    def run(self, pages: list[dict[str, Any]]) -> list[Any]:
        return list(
            self.client.batch_two_step_extract(
                images=[page["img_pil"] for page in pages],
                image_analysis=self.image_analysis,
            )
        )

    def close(self) -> None:
        del self.client
        del self.llm


class MinerU25Pro2604PdfStem:
    """Keep the lightweight document identity needed by assembly."""

    def run(self, paths: list[str]) -> list[str]:
        return [Path(path).stem for path in paths]


class MinerU25Pro2604AssembleDocument:
    """Assemble Pro 2604 pages after document-scoped lineage reduction."""

    def __init__(self, output_dir: str, parse_method: str = "vlm") -> None:
        _require_mineru_3()
        self.output_dir = output_dir
        self.parse_method = parse_method

    def run(
        self,
        grouped_contents: list[list[Any]],
        grouped_pages: list[list[dict[str, Any]]],
        stems: list[str],
    ) -> list[str]:
        from mineru.backend.utils.html_image_utils import (
            replace_inline_table_images,
        )
        from mineru.backend.utils.para_block_utils import (
            build_para_blocks_from_preproc,
            cleanup_internal_para_block_metadata,
            merge_para_text_blocks,
        )
        from mineru.backend.utils.runtime_utils import cross_page_table_merge
        from mineru.backend.vlm.vlm_magic_model import MagicModel
        from mineru.backend.vlm.vlm_middle_json_mkcontent import (
            union_make as vlm_union_make,
        )
        from mineru.data.data_reader_writer import FileBasedDataWriter
        from mineru.utils.config_reader import get_table_enable
        from mineru.utils.cut_image import cut_image_and_table
        from mineru.utils.enum_class import ContentType, MakeMode
        from mineru.utils.hash_utils import bytes_md5
        from mineru.utils.title_level_postprocess import (
            apply_title_leveling_to_pdf_info,
        )

        outputs = []
        for contents, pages, stem in zip(
            grouped_contents,
            grouped_pages,
            stems,
            strict=True,
        ):
            if len(contents) != len(pages):
                raise ValueError("MinerU content and page groups must align")
            markdown_dir = Path(self.output_dir) / stem / self.parse_method
            image_dir = markdown_dir / "images"
            image_dir.mkdir(parents=True, exist_ok=True)
            image_writer = FileBasedDataWriter(str(image_dir))
            markdown_writer = FileBasedDataWriter(str(markdown_dir))

            pdf_info = []
            for page_blocks, page in zip(contents, pages, strict=True):
                page_image = page["img_pil"]
                page_md5 = bytes_md5(page_image.tobytes())
                magic = MagicModel(
                    page_blocks,
                    page["page_width"],
                    page["page_height"],
                )
                image_blocks = magic.get_image_blocks()
                table_blocks = magic.get_table_blocks()
                chart_blocks = magic.get_chart_blocks()
                title_blocks = magic.get_title_blocks()
                discarded_blocks = magic.get_discarded_blocks()
                code_blocks = magic.get_code_blocks()
                ref_text_blocks = magic.get_ref_text_blocks()
                phonetic_blocks = magic.get_phonetic_blocks()
                list_blocks = magic.get_list_blocks()
                text_blocks = magic.get_text_blocks()
                equation_blocks = magic.get_interline_equation_blocks()
                for span in magic.get_all_spans():
                    if span["type"] in {
                        ContentType.IMAGE,
                        ContentType.TABLE,
                        ContentType.CHART,
                        ContentType.INTERLINE_EQUATION,
                    }:
                        cut_image_and_table(
                            span,
                            page_image,
                            page_md5,
                            page["page_id"],
                            image_writer,
                            scale=page["scale"],
                        )
                replace_inline_table_images(
                    table_blocks,
                    image_writer,
                    page["page_id"],
                )
                preproc_blocks = [
                    *image_blocks,
                    *table_blocks,
                    *chart_blocks,
                    *code_blocks,
                    *ref_text_blocks,
                    *phonetic_blocks,
                    *title_blocks,
                    *text_blocks,
                    *equation_blocks,
                    *list_blocks,
                ]
                preproc_blocks.sort(key=lambda block: block["index"])
                pdf_info.append(
                    {
                        "preproc_blocks": preproc_blocks,
                        "discarded_blocks": discarded_blocks,
                        "page_size": [
                            page["page_width"],
                            page["page_height"],
                        ],
                        "page_idx": page["page_id"],
                    }
                )

            build_para_blocks_from_preproc(pdf_info)
            merge_para_text_blocks(pdf_info)
            if get_table_enable(
                os.getenv("MINERU_VLM_TABLE_ENABLE", "True").lower() == "true"
            ):
                cross_page_table_merge(pdf_info)
            apply_title_leveling_to_pdf_info(pdf_info)
            cleanup_internal_para_block_metadata(pdf_info)
            middle = {
                "pdf_info": pdf_info,
                "_backend": "vlm",
                "_version_name": version("mineru"),
            }
            markdown = vlm_union_make(pdf_info, MakeMode.MM_MD, "images")
            content_list = vlm_union_make(
                pdf_info,
                MakeMode.CONTENT_LIST,
                "images",
            )
            markdown_writer.write_string(f"{stem}.md", markdown)
            markdown_writer.write_string(
                f"{stem}_content_list.json",
                json.dumps(content_list, ensure_ascii=False, indent=4),
            )
            (markdown_dir / "layout.json").write_text(
                json.dumps(middle, ensure_ascii=False, indent=4),
                encoding="utf-8",
            )
            outputs.append(f"{stem}.md")
        return outputs


__all__ = [
    "MinerU25Pro2604AssembleDocument",
    "MinerU25Pro2604PdfStem",
    "MinerU25Pro2604PdfToPages",
    "MinerU25Pro2604VlmPage",
]
