from .version import __version__, version_info
from .main import MineruEngine
from .legacy_pipeline import MineruEngineLegacy
from .dag_pipeline import MineruRayOrchDagEngine

__all__ = [
    "__version__",
    "version_info",
    "MineruEngine",
    "MineruEngineLegacy",
    "MineruRayOrchDagEngine",
]


def hello():
    return "Hello from flash-mineru!"
