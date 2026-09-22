"""Small compatibility hooks loaded by local vLLM worker processes."""

from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path


def _ignore_broken_optional_torchaudio() -> bool:
    """Keep an unrelated broken TorchAudio install out of image-only imports."""

    try:
        importlib.import_module("torchaudio")
        return False
    except Exception:
        # Transformers checks only whether TorchAudio can be discovered before
        # importing it. A system-site package compiled for another CUDA version
        # therefore breaks Qwen2-VL image processing even though this pipeline
        # never handles audio. Mark only that optional backend unavailable.
        for name in tuple(sys.modules):
            if name == "torchaudio" or name.startswith("torchaudio."):
                sys.modules.pop(name, None)

    # vLLM's EngineCore uses multiprocessing ``spawn`` inside a Ray actor. Its
    # interpreter imports vLLM/Transformers before loading general plugins, so
    # the parent-only function patch below is not early enough. Put a minimal
    # bundled shim first on both the current and child-process import paths.
    shim_root = str(Path(__file__).with_name("_compat_shims"))
    if shim_root not in sys.path:
        sys.path.insert(0, shim_root)
    pythonpath = os.environ.get("PYTHONPATH", "").split(os.pathsep)
    if shim_root not in pythonpath:
        os.environ["PYTHONPATH"] = os.pathsep.join(
            [shim_root, *(path for path in pythonpath if path)]
        )
    importlib.invalidate_caches()

    from transformers import utils as transformers_utils
    from transformers.utils import import_utils

    unavailable = lambda: False
    transformers_utils.is_torchaudio_available = unavailable
    import_utils.is_torchaudio_available = unavailable
    return True


def register_transformers_5_compatibility() -> None:
    """Restore the tokenizer view expected by CUDA-12 vLLM 0.10.

    MinerU 4 needs Transformers 5 for its Layout model, while vLLM 0.10
    still reads a few Transformers 4 compatibility attributes. vLLM loads
    this function through its general-plugin entry point in both the frontend
    and every spawned EngineCore process.
    """

    from transformers import PreTrainedTokenizerBase, __version__

    if int(__version__.split(".", 1)[0]) < 5:
        return

    if not hasattr(PreTrainedTokenizerBase, "all_special_tokens_extended"):
        PreTrainedTokenizerBase.all_special_tokens_extended = property(  # type: ignore[attr-defined]
            lambda tokenizer: list(tokenizer.all_special_tokens)
        )

    _ignore_broken_optional_torchaudio()

    # Transformers 5 stores Qwen2-VL's pixel bounds in ``size`` instead of
    # exposing the old direct attributes read by vLLM 0.10's profiler.
    from transformers.models.qwen2_vl.image_processing_qwen2_vl import (
        Qwen2VLImageProcessor,
    )

    def _size_value(processor: object, key: str) -> int:
        size = getattr(processor, "size")
        value = (
            size.get(key)
            if isinstance(size, dict)
            else getattr(size, key, None)
        )
        if value is None:
            raise AttributeError(f"Qwen2VL image processor size has no {key!r}")
        return int(value)

    if not hasattr(Qwen2VLImageProcessor, "min_pixels"):
        Qwen2VLImageProcessor.min_pixels = property(  # type: ignore[attr-defined]
            lambda processor: _size_value(processor, "shortest_edge")
        )
    if not hasattr(Qwen2VLImageProcessor, "max_pixels"):
        Qwen2VLImageProcessor.max_pixels = property(  # type: ignore[attr-defined]
            lambda processor: _size_value(processor, "longest_edge")
        )

    # Transformers 5 stopped mirroring the language-model fields from
    # ``text_config`` onto the outer multimodal config. vLLM 0.10 still passes
    # that outer object into its Qwen2 language-model implementation.
    from transformers.models.qwen2_vl.configuration_qwen2_vl import (
        Qwen2VLConfig,
    )

    forwarded_text_fields = (
        "vocab_size",
        "hidden_size",
        "intermediate_size",
        "num_hidden_layers",
        "num_attention_heads",
        "num_key_value_heads",
        "hidden_act",
        "max_position_embeddings",
        "max_window_layers",
        "rms_norm_eps",
    )
    for field in forwarded_text_fields:
        if not hasattr(Qwen2VLConfig, field):
            setattr(
                Qwen2VLConfig,
                field,
                property(
                    lambda config, field=field: getattr(
                        config.text_config, field
                    )
                ),
            )

    if not hasattr(Qwen2VLConfig, "rope_theta"):
        Qwen2VLConfig.rope_theta = property(  # type: ignore[attr-defined]
            lambda config: float(
                config.text_config.rope_parameters.get(
                    "rope_theta",
                    1_000_000.0,
                )
            )
        )
    if not hasattr(Qwen2VLConfig, "rope_parameters"):
        def _get_rope_parameters(config: object) -> dict[str, object]:
            stored = getattr(config, "_flash_mineru_rope_parameters", None)
            if stored is not None:
                return dict(stored)
            text_config = getattr(config, "text_config")
            return dict(getattr(text_config, "rope_parameters"))

        def _set_rope_parameters(
            config: object,
            value: dict[str, object] | None,
        ) -> None:
            object.__setattr__(
                config,
                "_flash_mineru_rope_parameters",
                dict(value or {}),
            )

        Qwen2VLConfig.rope_parameters = property(  # type: ignore[attr-defined]
            _get_rope_parameters,
            _set_rope_parameters,
        )


__all__ = ["register_transformers_5_compatibility"]
