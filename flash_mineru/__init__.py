from .version import __version__, version_info
from .main import MineruEngine
from .legacy_pipeline import MineruEngineLegacy
from .dag_pipeline import MineruRayOrchDagEngine
from .ray_utils import *

__all__ = [
    "__version__",
    "version_info",
    "MineruEngine",
    "MineruEngineLegacy",
    "MineruRayOrchDagEngine",
]


def hello():
    return "Hello from flash-mineru!"
