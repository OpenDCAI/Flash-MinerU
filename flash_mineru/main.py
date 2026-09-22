"""Published user API backed by the versioned RayOrch DAG engine.

The deprecated sequential implementation remains available as
:class:`flash_mineru.legacy_pipeline.MineruEngineLegacy`.
"""

from flash_mineru.dag_pipeline import MineruRayOrchDagEngine


class MineruEngine(MineruRayOrchDagEngine):
    """Run a selected MinerU pipeline through RayOrch.

    For the deprecated sequential path, see
    :class:`~flash_mineru.legacy_pipeline.MineruEngineLegacy`.
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(log_label="MineruEngine", **kwargs)
