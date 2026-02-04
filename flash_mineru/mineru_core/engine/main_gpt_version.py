# Copyright (c) OpenDataLab
# This file is derived from the MinerU project:
# https://github.com/opendatalab/MinerU
#
# Modifications and adaptations have been made by the Flash-MinerU authors.
#
# This file is licensed under the GNU Affero General Public License v3.0 (AGPL-3.0).
# You may obtain a copy of the License at:
#     https://www.gnu.org/licenses/agpl-3.0.html

from __future__ import annotations
from dataclasses import dataclass
from multiprocessing import Process, JoinableQueue, Event, Queue
from typing import List, Optional, Tuple
import multiprocessing as mp
import traceback
import time
import os

import json
import io

import pypdfium2 as pdfium
from io import BytesIO
from PIL import Image  # 仅在本进程内用，跨进程传 bytes
from vllm import LLM
from mineru_vl_utils import MinerUClient
from flash_mineru.data.data_reader_writer import FileBasedDataWriter
from flash_mineru.engine.model_output_to_middle_json import result_to_middle_json
from flash_mineru.engine.vlm_middle_json_mkcontent import union_make as vlm_union_make
from flash_mineru.utils.enum_class import MakeMode

# ---------- 基础数据模型 ----------
JobId = str
SENTINEL = ("__STOP__", -1)  # (job_id, page_idx) 特殊消息

@dataclass
class PdfJob:
    job_id: JobId
    pdf_path: str

@dataclass
class PageTask:
    job_id: JobId
    pdf_path: str
    page_index: int
    dpi: int = 200  # 可按需扩展

@dataclass
class RenderedPage:
    job_id: JobId
    page_index: int
    scale: float
    img_bytes: bytes  # 用 bytes 跨进程传递

@dataclass
class OcrResult:
    job_id: JobId
    page_index: int
    ocr_json: dict

@dataclass
class ParsedResult:
    job_id: JobId
    page_index: int
    parsed: dict

# ---------- 工具函数 ----------
def prepare_env(output_dir, pdf_file_name, parse_method):
    local_md_dir = str(os.path.join(output_dir, pdf_file_name, parse_method))
    local_image_dir = os.path.join(str(local_md_dir), "images")
    os.makedirs(local_image_dir, exist_ok=True)
    os.makedirs(local_md_dir, exist_ok=True)
    return local_image_dir, local_md_dir

def render_page_to_png_bytes(pdf_path: str, page_index: int, dpi: int=200) -> Tuple[bytes, float, tuple[float, float]]:
    doc = pdfium.PdfDocument(pdf_path)
    try:
        page = doc[page_index]
        scale = dpi / 72.0
        bmp = page.render(scale=scale)
        pil = bmp.to_pil()
        buf = BytesIO()
        pil.save(buf, "PNG")
        return buf.getvalue(), scale, page.get_size()
    finally:
        doc.close()

def fake_mineru_recognize(png_bytes: bytes) -> dict:
    # TODO: 替换为 MinerU 真实识别
    image = Image.open(io.BytesIO(png_bytes))
    llm = LLM(
        model="/mnt/DataFlow/models/opendatalab/MinerU2.5-2509-1.2B",
        # logits_processors=[MinerULogitsProcessor]  # if vllm>=0.10.1
        # dtype="float16",
        # enforce_eager=True
    )
    client = MinerUClient(
        backend="vllm-engine",
        vllm_llm=llm
    )
    extracted_blocks = client.two_step_extract(image)
    return extracted_blocks

def fake_parse_logic(ocr_json: dict, png_bytes, scale, image_writer, page_size, page_index, md_writer, pdf_file_name, image_dir) -> dict:
    # TODO: 替换为真实解析逻辑
    image = Image.open(io.BytesIO(png_bytes))
    image_dict = {'img_pil':image, 'scale':scale}
    middle_json = result_to_middle_json(ocr_json, image_dict, page_size, image_writer, page_index)
    md_content_str = vlm_union_make(middle_json["pdf_info"], MakeMode.MM_MD, image_dir)
    md_writer.write_string(
        f"{pdf_file_name}.md",
        md_content_str,
    )
    return middle_json

# ---------- 各流水线进程 ----------
class ControlProcess(Process):
    """看板：只读 progress_q 与 error_q，打印/统计"""
    def __init__(self, progress_q: Queue, error_q: Queue, stop_event: Event):
        super().__init__(daemon=True)
        self.progress_q = progress_q
        self.error_q = error_q
        self.stop_event = stop_event

    def run(self):
        while not self.stop_event.is_set():
            # 错误优先处理
            try:
                while True:
                    err = self.error_q.get_nowait()
                    print("[ERROR]", err)
                    self.stop_event.set()
            except Exception:
                pass

            # 进度
            try:
                msg = self.progress_q.get(timeout=0.2)
                print("[PROGRESS]", msg)
            except Exception:
                pass

# PDF → PageTask（拆页）
class PDF2ImageProcess(Process):
    def __init__(self, in_q: JoinableQueue, out_q: JoinableQueue, progress_q: Queue, error_q: Queue, stop_event: Event):
        super().__init__(daemon=True)
        self.in_q = in_q
        self.out_q = out_q
        self.progress_q = progress_q
        self.error_q = error_q
        self.stop_event = stop_event

    def run(self):
        try:
            while True:
                job = self.in_q.get()
                if job == SENTINEL:
                    # 把哨兵原样传给下游同样数量的消费者由主控负责，这里只完成入队项
                    self.in_q.task_done()
                    break
                assert isinstance(job, PdfJob)
                # 拆页
                try:
                    doc = pdfium.PdfDocument(job.pdf_path)
                    n = len(doc); doc.close()
                    self.progress_q.put({"stage":"pdf2image/split", "job":job.job_id, "pages":n})
                    for i in range(n):
                        self.out_q.put(PageTask(job.job_id, job.pdf_path, i))
                except Exception as e:
                    self.error_q.put(f"split failed: {job.pdf_path} | {e}\n{traceback.format_exc()}")
                    self.stop_event.set()
                finally:
                    self.in_q.task_done()
        except KeyboardInterrupt:
            pass

# PageTask → RenderedPage（渲染）
class Mineru2_5RecognizeProcess(Process):
    def __init__(self, in_q: JoinableQueue, out_q: JoinableQueue, progress_q: Queue, error_q: Queue, stop_event: Event):
        super().__init__(daemon=True)
        self.in_q = in_q
        self.out_q = out_q
        self.progress_q = progress_q
        self.error_q = error_q
        self.stop_event = stop_event

    def run(self):
        while not self.stop_event.is_set():
            item = self.in_q.get()
            if item == SENTINEL:
                self.in_q.task_done()
                break
            try:
                assert isinstance(item, PageTask)
                png, scale = render_page_to_png_bytes(item.pdf_path, item.page_index, item.dpi)
                ocr_json = fake_mineru_recognize(png)
                self.out_q.put(OcrResult(item.job_id, item.page_index, ocr_json))
                # 进度上报（可降采样）
                if item.page_index % 10 == 0:
                    self.progress_q.put({"stage":"recognize", "job":item.job_id, "page":item.page_index})
            except Exception as e:
                self.error_q.put(f"recognize failed: {item} | {e}\n{traceback.format_exc()}")
                self.stop_event.set()
            finally:
                self.in_q.task_done()

# OcrResult → ParsedResult（解析）
class ResultParseProcess(Process):
    def __init__(self, in_q: JoinableQueue, out_q: JoinableQueue, progress_q: Queue, error_q: Queue, stop_event: Event):
        super().__init__(daemon=True)
        self.in_q = in_q
        self.out_q = out_q
        self.progress_q = progress_q
        self.error_q = error_q
        self.stop_event = stop_event

    def run(self):
        while not self.stop_event.is_set():
            item = self.in_q.get()
            if item == SENTINEL:
                self.in_q.task_done()
                break
            try:
                assert isinstance(item, OcrResult)
                parsed = fake_parse_logic(item.ocr_json)
                self.out_q.put(ParsedResult(item.job_id, item.page_index, parsed))
                if item.page_index % 10 == 0:
                    self.progress_q.put({"stage":"output", "job":item.job_id, "page":item.page_index})
            except Exception as e:
                self.error_q.put(f"parse failed: {item} | {e}\n{traceback.format_exc()}")
                self.stop_event.set()
            finally:
                self.in_q.task_done()

# ---------- 主协调类 ----------
class FlashMineruMain:
    def __init__(self, num_splitters=1, num_recognizers=1, num_parsers=1):
        mp.set_start_method("spawn", force=True)  # 跨平台一致
        # 队列：合理的 maxsize 做背压
        self.q_jobs      = JoinableQueue(maxsize=64)    # PdfJob
        self.q_pages     = JoinableQueue(maxsize=256)   # PageTask
        self.q_ocr       = JoinableQueue(maxsize=256)   # OcrResult
        self.q_parsed    = JoinableQueue(maxsize=128)   # ParsedResult（最终）
        self.progress_q  = Queue()
        self.error_q     = Queue()
        self.stop_event  = Event()

        # 进程池
        self.ctrl = ControlProcess(self.progress_q, self.error_q, self.stop_event)
        self.splitters: List[Process] = [PDF2ImageProcess(self.q_jobs, self.q_pages, self.progress_q, self.error_q, self.stop_event) for _ in range(num_splitters)]
        self.recognizers: List[Process] = [Mineru2_5RecognizeProcess(self.q_pages, self.q_ocr, self.progress_q, self.error_q, self.stop_event) for _ in range(num_recognizers)]
        self.parsers: List[Process] = [ResultParseProcess(self.q_ocr, self.q_parsed, self.progress_q, self.error_q, self.stop_event) for _ in range(num_parsers)]

    def start(self):
        self.ctrl.start()
        for p in self.splitters + self.recognizers + self.parsers:
            p.start()

    def run(self, pdf_path_list: List[str], job_id: Optional[JobId] = None) -> List[ParsedResult]:
        job_id = job_id or f"job-{int(time.time())}"
        self.start()

        # 入队 PDF 任务
        self.progress_q.put({"stage":"start", "job":job_id, "total_pdfs": len(pdf_path_list)})
        for p in pdf_path_list:
            self.q_jobs.put(PdfJob(job_id, p))

        # 发送第一级哨兵（按 splitter 数量）
        for _ in self.splitters:
            self.q_jobs.put(SENTINEL)

        # 等第一级处理完（拆页）
        self.q_jobs.join()
        # 把哨兵继续向下游传（每个队列按消费者数量注入）
        for _ in self.recognizers:
            self.q_pages.put(SENTINEL)
        self.q_pages.join()

        for _ in self.parsers:
            self.q_ocr.put(SENTINEL)
        # 收集最终结果（也可在独立线程/进程里异步消费）
        results: List[ParsedResult] = []
        # 这里简单地轮询直到解析队列空且上游 join 完成
        while True:
            try:
                item = self.q_parsed.get(timeout=0.2)
                results.append(item)
            except Exception:
                # 判断是否完成（上游都 join 了，并且解析进程也会在收到哨兵后退出）
                if all(not p.is_alive() for p in self.parsers) and self.q_ocr.empty():
                    break

            # 错误处理：一旦发现，立即停止
            try:
                err = self.error_q.get_nowait()
                self.stop_event.set()
                raise RuntimeError(err)
            except Exception:
                pass

        self.cleanup()
        return sorted(results, key=lambda r: (r.job_id, r.page_index))

    def cleanup(self):
        self.stop_event.set()
        # 确保所有 joinable 队列被释放
        for q in (self.q_jobs, self.q_pages, self.q_ocr):
            try:
                q.close(); q.join_thread()
            except Exception:
                pass

        for p in self.splitters + self.recognizers + self.parsers + [self.ctrl]:
            if p.is_alive():
                p.join(timeout=1.0)
                if p.is_alive():
                    p.terminate()
                    
                    
if __name__ == '__main__':
    path = '/mnt/DataFlow/junzhu/Flash-MinerU/flash_mineru/test/test.pdf'
    # test = FlashMineruMain()
    # ans = test.run(['/data1/ljz/Flash-MinerU/flash_mineru/test/test.pdf'])
    # print(ans)
    local_image_dir, local_md_dir = prepare_env('./outputs', 'test.pdf', 'vlm')
    
    image_writer, md_writer = FileBasedDataWriter(local_image_dir), FileBasedDataWriter(local_md_dir)
    png, scale, page_size = render_page_to_png_bytes(path, 1)
    ocr_json = fake_mineru_recognize(png)
    ans = fake_parse_logic(
            ocr_json=ocr_json,
            png_bytes=png, 
            image_writer=image_writer, 
            page_size=page_size, 
            page_index=0, 
            scale=scale,
            md_writer=md_writer,
            image_dir=local_image_dir,
            pdf_file_name='test.pdf'
        )

    print(ans)
    
