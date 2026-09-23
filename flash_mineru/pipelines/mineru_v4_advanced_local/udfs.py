"""UDFs for one complete MinerU 4 Advanced model stack per GPU."""

from __future__ import annotations

from typing import Any

from flash_mineru.pipelines._shared.mineru_v4 import (
    MinerU4RenderWindow,
    MinerU4WindowAnalyzer,
    enable_local_vllm_compatibility,
    save_mineru_4_documents,
    split_pdf_windows,
)


class MinerU4AdvancedLocalSplitPdf:
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


class MinerU4AdvancedLocalRenderWindow(MinerU4RenderWindow):
    """Render one ordered PDF window on a CPU actor."""


class MinerU4AdvancedLocalAnalyzeWindow(MinerU4WindowAnalyzer):
    """Keep Layout, Two-Step VLM, MFR, and OCR in one GPU actor."""

    def __init__(
        self,
        *,
        vlm_server_url: str = "",
        vlm_engine: str = "vllm",
        **kwargs: Any,
    ) -> None:
        if vlm_server_url:
            raise ValueError(
                "v4-advanced-local requires an actor-local VLM; "
                "vlm_server_url must be empty"
            )
        if vlm_engine != "vllm":
            raise ValueError(
                "v4-advanced-local currently requires vlm_engine='vllm'"
            )
        enable_local_vllm_compatibility()
        super().__init__(
            effort="xhigh",
            vlm_server_url="",
            vlm_engine=vlm_engine,
            **kwargs,
        )


class MinerU4AdvancedLocalAssembleDocument:
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
    "MinerU4AdvancedLocalAnalyzeWindow",
    "MinerU4AdvancedLocalAssembleDocument",
    "MinerU4AdvancedLocalRenderWindow",
    "MinerU4AdvancedLocalSplitPdf",
]
