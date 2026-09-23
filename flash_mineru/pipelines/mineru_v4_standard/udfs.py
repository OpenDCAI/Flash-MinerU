"""UDFs for the MinerU 4 Standard pipeline."""

from __future__ import annotations

from typing import Any

from flash_mineru.pipelines._shared.mineru_v4 import (
    MinerU4RenderWindow,
    MinerU4WindowAnalyzer,
    save_mineru_4_documents,
    split_pdf_windows,
)


class MinerU4StandardSplitPdf:
    """Split each PDF into ordered, independently scheduled windows."""

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
            effort="high",
            parse_mode=self.parse_mode,
            window_size=self.window_size,
        )


class MinerU4StandardAnalyzeWindow(MinerU4WindowAnalyzer):
    """Run local layout then layout-guided VLM extraction."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(effort="high", **kwargs)


class MinerU4StandardRenderWindow(MinerU4RenderWindow):
    """Render one ordered PDF window on a CPU actor."""


class MinerU4StandardAssembleDocument:
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
            tier="standard",
            effort="high",
        )


__all__ = [
    "MinerU4StandardAnalyzeWindow",
    "MinerU4StandardAssembleDocument",
    "MinerU4StandardRenderWindow",
    "MinerU4StandardSplitPdf",
]
