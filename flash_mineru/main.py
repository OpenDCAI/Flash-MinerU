"""
Published user API: :class:`MineruEngine` only wires the v1.0.0 DAG path from :mod:`flash_mineru.dag_pipeline`.

v0.0.4 lives in :mod:`flash_mineru.legacy_pipeline` (:class:`MineruEngineLegacy`).
"""

from flash_mineru.dag_pipeline import MineruRayOrchDagEngine


class MineruEngine(MineruRayOrchDagEngine):
    """Default Flash-MinerU engine (v1.0.0): pipeline-parallel PDF → Markdown.

    Implemented in :mod:`flash_mineru.dag_pipeline`. For the deprecated sequential path see
    :class:`~flash_mineru.legacy_pipeline.MineruEngineLegacy`.
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(log_label="MineruEngine", **kwargs)
