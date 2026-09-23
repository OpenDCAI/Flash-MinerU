import json
import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from benchmark_utils import (  # noqa: E402
    locate_v25_middle,
    shard_by_page_count,
    validate_v25_outputs,
)


def test_v25_middle_accepts_flash_and_native_names(tmp_path: Path):
    output = tmp_path / "doc" / "vlm"
    output.mkdir(parents=True)
    native = output / "doc_middle.json"
    native.write_text("{}", encoding="utf-8")
    assert locate_v25_middle(output, "doc") == native

    flash = output / "layout.json"
    flash.write_text("{}", encoding="utf-8")
    assert locate_v25_middle(output, "doc") == flash


def test_validate_v25_accepts_native_middle_name(tmp_path: Path):
    pdf = tmp_path / "doc.pdf"
    pdf.write_bytes(b"not read by output validation")
    output = tmp_path / "outputs" / "doc" / "vlm"
    output.mkdir(parents=True)
    (output / "doc.md").write_text("# document\n", encoding="utf-8")
    (output / "doc_content_list.json").write_text("[]", encoding="utf-8")
    (output / "doc_middle.json").write_text(
        json.dumps({"pdf_info": [{}, {}]}),
        encoding="utf-8",
    )

    assert validate_v25_outputs(tmp_path / "outputs", [pdf]) == {
        "documents": 1,
        "successful_documents": 1,
        "pages": 2,
        "failed_documents": [],
    }


def test_page_count_sharding_balances_long_documents(monkeypatch, tmp_path: Path):
    paths = [tmp_path / f"{index}.pdf" for index in range(6)]
    pages = dict(zip(paths, (10, 9, 8, 3, 2, 2), strict=True))
    monkeypatch.setattr(
        "benchmark_utils.pdf_page_count",
        lambda path: pages[path],
    )

    shards = shard_by_page_count(paths, 2)

    assert [sum(pages[path] for path in shard) for shard in shards] == [17, 17]
    assert sorted(path for shard in shards for path in shard) == sorted(paths)
