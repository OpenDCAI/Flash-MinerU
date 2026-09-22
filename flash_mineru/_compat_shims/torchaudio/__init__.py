"""No-op TorchAudio shim for Flash-MinerU's image-only local VLM workers.

This module is placed on the child-process import path only when importing the
real optional TorchAudio installation has already failed.
"""

__version__ = "0.0"
