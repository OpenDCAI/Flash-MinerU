"""Page-grained UDFs for the stable MinerU 2.5 VLM pipeline."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class MinerU25PdfToPages:
    """Render each PDF into ordered page records on persistent CPU actors."""

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


class MinerU25VlmPage:
    """Run MinerU 2.5 two-step extraction over cross-document page batches."""

    def __init__(
        self,
        model: str,
        gpu_memory_utilization: float = 0.9,
        image_analysis: bool = False,
    ) -> None:
        from mineru_vl_utils import MinerUClient, MinerULogitsProcessor
        from vllm import LLM

        self.llm = LLM(
            model=model,
            gpu_memory_utilization=gpu_memory_utilization,
            # MinerU enables this processor for vLLM >= 0.10.1 to stop
            # pathological repeated-token generations. Keep the versioned
            # Flash pipeline aligned with the native MinerU VLM backend.
            logits_processors=[MinerULogitsProcessor],
        )
        self.client = MinerUClient(
            backend="vllm-engine",
            vllm_llm=self.llm,
            image_analysis=image_analysis,
        )
        self.image_analysis = image_analysis

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


class MinerU25PdfStem:
    """Keep only the lightweight document identity needed by assembly."""

    def run(self, paths: list[str]) -> list[str]:
        return [Path(path).stem for path in paths]


class MinerU25AssembleDocument:
    """Convert ordered MinerU page results into the historical output files."""

    def __init__(self, output_dir: str, parse_method: str = "vlm") -> None:
        self.output_dir = output_dir
        self.parse_method = parse_method

    def run(
        self,
        grouped_contents: list[list[Any]],
        grouped_pages: list[list[dict[str, Any]]],
        stems: list[str],
    ) -> list[str]:
        from flash_mineru.mineru_core.data.data_reader_writer import (
            FileBasedDataWriter,
        )
        from flash_mineru.mineru_core.engine.model_output_to_middle_json import (
            result_to_middle_json,
        )
        from flash_mineru.mineru_core.engine.vlm_middle_json_mkcontent import (
            union_make as vlm_union_make,
        )
        from flash_mineru.mineru_core.utils.enum_class import MakeMode

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

            middle = result_to_middle_json(
                list(contents),
                list(pages),
                image_writer,
            )
            markdown = vlm_union_make(
                middle["pdf_info"],
                MakeMode.MM_MD,
                "images",
            )
            content_list = vlm_union_make(
                middle["pdf_info"],
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
    "MinerU25AssembleDocument",
    "MinerU25PdfStem",
    "MinerU25PdfToPages",
    "MinerU25VlmPage",
]
