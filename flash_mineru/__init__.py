from .version import __version__, version_info
from .main import MineruEngine
from .legacy_pipeline import MineruEngineLegacy
from .dag_pipeline import MineruRayOrchDagEngine
from .pipelines import get_pipeline_spec, pipeline_names, prepare_pipeline

__all__ = [
    "__version__",
    "version_info",
    "MineruEngine",
    "MineruEngineLegacy",
    "MineruRayOrchDagEngine",
    "get_pipeline_spec",
    "pipeline_names",
    "prepare_pipeline",
]


def hello():
    return "Hello from flash-mineru!"
