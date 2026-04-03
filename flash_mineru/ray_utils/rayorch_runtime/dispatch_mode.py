from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Callable, Dict, Mapping, Sequence, Tuple, Union, List

import ray


class Dispatch(Enum):
    BROADCAST = auto()
    PER_REPLICA = auto()
    # DIRECT_ROLLOUT_METHOD = auto()
    SHARD_CONTIGUOUS = auto()
    # ALL_SLICED_TO_TRIPLET_ALL = auto()


# VERL风格：第一个参数是“worker_group”，这里我们传 RayModule 自己（或module view）
DispatchFn = Callable[[Any, Any], Tuple[Tuple[Any, ...], Dict[str, Any]]]
# 上面这个 Any, Any 写法太松了，下面给更准确的：
# DispatchFn = Callable[[Any, ...], Tuple[Tuple[Any, ...], Dict[str, Any]]]
CollectFn = Callable[[Any, Any], Any]


@dataclass(frozen=True)
class DispatchSpec:
    dispatch_fn: DispatchFn
    collect_fn: CollectFn


@dataclass(frozen=True)
class ShardedObjectRef:
    """
    Per-replica view of one shared ObjectRef.

    The underlying ref points to a sequence-like batch (list/tuple). Each replica
    resolves and slices its own contiguous shard at execution time.
    """

    ref: ray.ObjectRef
    shard_idx: int
    num_shards: int


# --------- VERL 风格实现 ---------
def dispatch_broadcast(rm, *args, **kwargs):
    ws = rm._replicas  # hard code to avoid circular import
    args = tuple([arg] * ws for arg in args)
    kwargs = {k: [v] * ws for k, v in kwargs.items()}
    return args, kwargs


def dispatch_per_replica(rm, *args, **kwargs):
    # 约定：args/kwargs 已经是 per-replica 形式
    return args, kwargs


def collect_identity(rm, output):
    return output


# dispatch_mode.py (append)

def _is_shardable(x: Any) -> bool:
    # 只把 list/tuple 当作可切片 batch；其他一律广播
    # 如果你还想支持 numpy/torch batch，可在这里扩展
    return isinstance(x, (list, tuple))


def _is_object_ref(x: Any) -> bool:
    return isinstance(x, ray.ObjectRef)


def dispatch_shard_all_args_mod(rm, *args, **kwargs):
    """
    将所有可切片参数（list/tuple）按「顺序连续切分」分发到不同 actor，
    其他参数广播到所有 actor。

    - shardable 参数必须长度一致
    - 切分策略：前面的 rank 多 1 个（如果不能整除）
    - collect_concat 按 rank 顺序拼接即可保证整体顺序稳定

    返回：
      per_args: tuple，每个元素是 len=replicas 的 list，表示每个 rank 的该参数值
      per_kwargs: dict，每个 value 是 len=replicas 的 list
    """
    ws = rm._replicas

    # 1) 收集所有 shardable 参数长度；ObjectRef 也视为“可分片输入”，但长度在 worker 端再解析。
    lengths = []
    has_ref_input = False
    for a in args:
        if _is_shardable(a):
            lengths.append(len(a))
        elif _is_object_ref(a):
            has_ref_input = True
    for v in kwargs.values():
        if _is_shardable(v):
            lengths.append(len(v))
        elif _is_object_ref(v):
            has_ref_input = True

    if not lengths and not has_ref_input:
        # 无 shardable 参数且无 ref 参数：广播
        per_args = tuple([a] * ws for a in args)
        per_kwargs = {k: [v] * ws for k, v in kwargs.items()}
        return per_args, per_kwargs

    ranges = None
    if lengths:
        n = lengths[0]
        if any(l != n for l in lengths):
            raise ValueError(f"Shardable args/kwargs must have same length. Got lengths={lengths}")

        # 2) 计算每个 rank 的切片区间（连续）
        #    例如 n=10, ws=3 -> sizes = [4,3,3]
        base = n // ws
        rem = n % ws
        sizes = [base + (1 if i < rem else 0) for i in range(ws)]

        # 生成每个 rank 的 [start, end)
        ranges = []
        start = 0
        for sz in sizes:
            end = start + sz
            ranges.append((start, end))
            start = end

    # 3) 切 shardable 参数；ObjectRef 用 per-replica 视图，延迟到 worker 端解引用切片。
    def shard_seq(seq):
        if ranges is None:
            raise ValueError("Internal error: ranges is None for shardable sequence input.")
        return [seq[s:e] for (s, e) in ranges]

    def shard_ref(ref: ray.ObjectRef):
        return [ShardedObjectRef(ref=ref, shard_idx=i, num_shards=ws) for i in range(ws)]

    # 4) per_args
    per_args_list = []
    for a in args:
        if _is_shardable(a):
            per_args_list.append(shard_seq(a))
        elif _is_object_ref(a):
            per_args_list.append(shard_ref(a))
        else:
            per_args_list.append([a] * ws)

    # 5) per_kwargs
    per_kwargs = {}
    for k, v in kwargs.items():
        if _is_shardable(v):
            per_kwargs[k] = shard_seq(v)
        elif _is_object_ref(v):
            per_kwargs[k] = shard_ref(v)
        else:
            per_kwargs[k] = [v] * ws

    return tuple(per_args_list), per_kwargs



def collect_concat(rm, outputs):
    """
    递归 concat reduce，适配 “拆 shard 再拼回去” 的 batch 风格。

    支持输出类型：
      - list/tuple: 视为 batch 序列，按 rank 顺序拼接
      - dict: 每个 value 再递归合并（典型：{k: list_batch})
      - tuple/list of multiple outputs: (images, meta) / (masks, meta) / ...
      - None: 忽略

    约束/假设（符合你说的协议）：
      - 所有 shardable 的字段都是 list 或 list-of-dict / list-of-list-of-dict
      - 各字段长度一致（如果是 dict 多字段）
      - 拼接顺序按 outputs 的 rank 顺序
    """
    # RayModule 的 _fanout 里输出通常是：outputs = ray.get(refs)
    if not outputs:
        return None

    # 过滤 None shard
    outs = [o for o in outputs if o is not None]
    if not outs:
        return None

    def _is_seq(x: Any) -> bool:
        # list/tuple 是“可能需要 batch 拼接”的序列；str/bytes 不算
        return isinstance(x, (list, tuple)) and not isinstance(x, (str, bytes))

    def _merge_dict(dict_list: List[Dict[str, Any]]) -> Dict[str, Any]:
        # key 的集合必须一致（强约束，便于 debug）
        keys0 = set(dict_list[0].keys())
        for d in dict_list[1:]:
            if set(d.keys()) != keys0:
                raise ValueError(f"collect_concat: dict keys mismatch: {set(d.keys())} vs {keys0}")

        merged: Dict[str, Any] = {}
        for k in keys0:
            merged[k] = _merge([d[k] for d in dict_list])
        return merged

    def _merge(seq: List[Any]) -> Any:
        """
        递归合并一组同构对象（来自不同 replicas 的同一输出槽位）
        """
        # 去掉 None
        seq = [x for x in seq if x is not None]
        if not seq:
            return None

        x0 = seq[0]

        # 1) dict：对每个 key 递归 merge
        if isinstance(x0, dict):
            if not all(isinstance(x, dict) for x in seq):
                raise ValueError("collect_concat: mixed types in dict merge")
            return _merge_dict(seq)  # type: ignore[arg-type]

        # 2) tuple/list：要区分 “batch list” vs “multi-output tuple/list”
        if _is_seq(x0):
            # 如果是 tuple：通常表示多输出，比如 (images, meta)
            if isinstance(x0, tuple):
                # 要求所有都是同长度 tuple
                if not all(isinstance(x, tuple) and len(x) == len(x0) for x in seq):
                    raise ValueError("collect_concat: tuple outputs must have same arity")
                merged_items = []
                for i in range(len(x0)):
                    merged_items.append(_merge([x[i] for x in seq]))  # 逐槽位递归
                return tuple(merged_items)

            # 如果是 list：这里既可能是 batch，也可能是 multi-output(list形态)
            # 我们用一个规则：
            # - 如果所有 replicas 的 list 都是 “同构元素” 并且元素不是 dict/tuple/list -> 视为 batch 直接 extend
            # - 如果 list 的长度固定且用于多输出（不太常见），你可以用 tuple 输出避免歧义
            # 在你的协议里，list 基本就是 batch，所以默认 extend。
            out_list: List[Any] = []
            for x in seq:
                if not isinstance(x, list):
                    raise ValueError("collect_concat: mixed types in list merge")
                out_list.extend(x)
            return out_list

        # 3) 标量/对象：默认保持第一个（广播参数/常量）
        # 例如某些 op 可能返回一个 str / int
        return x0

    merged = _merge(outs)

    # 可选：如果最终是 dict 且含多个 list 字段，检查长度一致（符合你“等长字段”设定）
    if isinstance(merged, dict):
        lens = []
        for v in merged.values():
            if isinstance(v, list):
                lens.append(len(v))
        if lens and any(l != lens[0] for l in lens):
            raise ValueError(f"collect_concat: merged dict list lengths not equal: {lens}")

    return merged

def collect_triplet_concat(rm, outputs):
    """
    outputs: List[Tuple[main, args, kwargs]]
    - main: 期望是 list（可 concat）
    - args/kwargs: 透传第一个（要求各 shard 一致；通常广播参数就是一致的）
    """
    if not outputs:
        return None, (), {}

    mains = []
    first_args = outputs[0][1]
    first_kwargs = outputs[0][2]

    for main, _args, _kwargs in outputs:
        if main is None:
            continue
        if isinstance(main, list):
            mains.extend(main)
        else:
            mains.append(main)

    return mains, first_args, first_kwargs

DISPATCH_MODE_FN_REGISTRY = {
    Dispatch.BROADCAST: DispatchSpec(dispatch_broadcast, collect_identity),
    Dispatch.PER_REPLICA: DispatchSpec(dispatch_per_replica, collect_identity),
    Dispatch.SHARD_CONTIGUOUS: DispatchSpec(dispatch_shard_all_args_mod, collect_concat),
    # Dispatch.ALL_SLICED_TO_TRIPLET_ALL: DispatchSpec(dispatch_shard_all_args_mod, collect_triplet_concat),
}




DispatchMode = Union[Dispatch, Mapping[str, Any]]


def _check_dispatch_mode(dispatch_mode: DispatchMode):
    if isinstance(dispatch_mode, Dispatch):
        return
    if isinstance(dispatch_mode, Mapping):
        for k in ("dispatch_fn", "collect_fn"):
            if k not in dispatch_mode:
                raise ValueError(f"custom dispatch_mode must contain key '{k}'")
        return
    raise TypeError(f"dispatch_mode must be Dispatch or Mapping. Got {type(dispatch_mode)}")


def get_predefined_dispatch_fn(dispatch_mode: DispatchMode) -> DispatchSpec:
    _check_dispatch_mode(dispatch_mode)
    if isinstance(dispatch_mode, Dispatch):
        return DISPATCH_MODE_FN_REGISTRY[dispatch_mode]
    return DispatchSpec(dispatch_mode["dispatch_fn"], dispatch_mode["collect_fn"])  # type: ignore[index]


def update_dispatch_mode(dispatch_mode: Dispatch, dispatch_fn, collect_fn):
    DISPATCH_MODE_FN_REGISTRY[dispatch_mode] = DispatchSpec(dispatch_fn, collect_fn)
