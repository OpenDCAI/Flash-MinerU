"""Vendored RayOrch DAG stack (in-repo snapshot).

Legacy Flash path uses ``flash_mineru.ray_utils`` (``dispatch_ray_module.RayModule``).
This subpackage is separate so DAG ``RayModule`` / ``Dispatch`` do not shadow the legacy API.
"""
from __future__ import annotations

from .version import __version__, version_info
from .dispatch_mode import (
    DispatchMode,
    get_predefined_dispatch_fn,
    Dispatch,
)
from .ray_module import RayModule

RayModuleFuture = RayModule.RayModuleFuture
from .dag_new_pipeline import (
    Pipeline,
    DagPipeline,
    PipeRef,
    DagExecutor,
    Executor,
    SequentialExecutor,
)

DagNewPipeRef = PipeRef

__all__ = [
    "__version__",
    "version_info",
    "DispatchMode",
    "get_predefined_dispatch_fn",
    "Dispatch",
    "RayModule",
    "RayModuleFuture",
    "Pipeline",
    "DagPipeline",
    "PipeRef",
    "DagNewPipeRef",
    "DagExecutor",
    "Executor",
    "SequentialExecutor",
]
