"""UDFs for the MinerU 4 Flash pipeline."""

from __future__ import annotations

from typing import Any

from flash_mineru.pipelines._shared.mineru_v4 import (
    MinerU4WindowAnalyzer,
    save_mineru_4_documents,
    split_pdf_windows,
)


class MinerU4FlashSplitPdf:
    """Split PDFs into lineage-tracked windows.

    Flash TXT remains one document-sized child because its native DocVortex
    path has no expensive page-model stage. Flash OCR uses normal windows.
    """

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
            effort="flash",
            parse_mode=self.parse_mode,
            window_size=self.window_size,
        )


class MinerU4FlashAnalyzeWindow(MinerU4WindowAnalyzer):
    """Run the official MinerU 4 Flash stages."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(effort="flash", **kwargs)


class MinerU4FlashAssembleDocument:
    """Restore document order and save official MinerU 4 outputs."""

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
            tier="flash",
            effort="flash",
        )


__all__ = [
    "MinerU4FlashAnalyzeWindow",
    "MinerU4FlashAssembleDocument",
    "MinerU4FlashSplitPdf",
]
