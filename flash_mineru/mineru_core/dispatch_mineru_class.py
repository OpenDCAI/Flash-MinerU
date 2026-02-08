# Copyright (c) OpenDataLab
# This file is derived from the MinerU project:
# https://github.com/opendatalab/MinerU
#
# Modifications and adaptations have been made by the Flash-MinerU authors.
#
# This file is licensed under the GNU Affero General Public License v3.0 (AGPL-3.0).
# You may obtain a copy of the License at:
#     https://www.gnu.org/licenses/agpl-3.0.html
import json
import os, sys, io
from vllm import LLM
from mineru_vl_utils import MinerUClient

from pathlib import Path
from typing import List, Optional, Tuple
from loguru import logger
import pypdfium2 as pdfium
from flash_mineru.mineru_core.utils.pdf_image_tools import load_images_from_pdf
from flash_mineru.mineru_core.data.data_reader_writer import FileBasedDataWriter
from flash_mineru.mineru_core.engine.model_output_to_middle_json import result_to_middle_json
from flash_mineru.mineru_core.engine.vlm_middle_json_mkcontent import union_make as vlm_union_make
from flash_mineru.mineru_core.utils.enum_class import MakeMode

def get_end_page_id(end_page_id, pdf_page_num):
    end_page_id = end_page_id if end_page_id is not None and end_page_id >= 0 else pdf_page_num - 1
    if end_page_id > pdf_page_num - 1:
        logger.warning("end_page_id is out of range, use images length")
        end_page_id = pdf_page_num - 1
    return end_page_id

def convert_pdf_bytes_to_bytes_by_pypdfium2(pdf_bytes, start_page_id=0, end_page_id=None):
    pdf = pdfium.PdfDocument(pdf_bytes)
    output_pdf = pdfium.PdfDocument.new()
    try:
        end_page_id = get_end_page_id(end_page_id, len(pdf))

        # 逐页导入,失败则跳过
        output_index = 0
        for page_index in range(start_page_id, end_page_id + 1):
            try:
                output_pdf.import_pages(pdf, pages=[page_index])
                output_index += 1
            except Exception as page_error:
                output_pdf.del_page(output_index)
                logger.warning(f"Failed to import page {page_index}: {page_error}, skipping this page.")
                continue

        # 将新PDF保存到内存缓冲区
        output_buffer = io.BytesIO()
        output_pdf.save(output_buffer)

        # 获取字节数据
        output_bytes = output_buffer.getvalue()
    except Exception as e:
        logger.warning(f"Error in converting PDF bytes: {e}, Using original PDF bytes.")
        output_bytes = pdf_bytes
    pdf.close()
    output_pdf.close()
    
    return output_bytes, end_page_id

def prepare_env(output_dir, pdf_file_name, parse_method):
    local_md_dir = str(os.path.join(output_dir, pdf_file_name, parse_method))
    local_image_dir = os.path.join(str(local_md_dir), "images")
    os.makedirs(local_image_dir, exist_ok=True)
    os.makedirs(local_md_dir, exist_ok=True)
    return local_image_dir, local_md_dir

class Pdf2ImageOp:
    
    def __init__(self):
        pass
    
    def run(self, pdf_path_list: List[str], start_page_id: int = 0, end_page_id: int = None):
        images = []
        for path in pdf_path_list:
            if not isinstance(path, Path):
                path = Path(path)
            image = []
            with open(str(path), "rb") as input_file:
                file_bytes = input_file.read()
                pdf_bytes, end_page_id = convert_pdf_bytes_to_bytes_by_pypdfium2(file_bytes, start_page_id, end_page_id)
                images_list, pdf_doc = load_images_from_pdf(pdf_bytes, image_type='pil_img', start_page_id=start_page_id, end_page_id=end_page_id)
                counter = start_page_id
                for img in images_list:
                    if img is None:
                        continue
                    width, height = map(int, pdf_doc[counter].get_size())
                    img.update({'pdf_path': str(path), 'page_id': counter, 'page_width': width, 'page_height': height, 'pdf_len': end_page_id - start_page_id + 1})
                    image.append(img)
                    counter += 1
                images.append([image])

        return images
    
class ProcessImagesOp:
    
    def __init__(
        self, 
        model="/home/dataset-local/models/MinerU2.5-2509-1.2B",
        gpu_memory_utilization=0.2
    ):
        llm = LLM(
            model=model,
            gpu_memory_utilization=gpu_memory_utilization,
            # logits_processors=[MinerULogitsProcessor]  # if vllm>=0.10.1
            # dtype="float16",
            # enforce_eager=True
        )
        self.llm = llm
        self.client = MinerUClient(
            backend="vllm-engine",
            vllm_llm=llm
        )
        
    def run(self, images):
        output = []
        for idx, images_list in enumerate(images):
            images_list = images_list[0]
            images_pil_list = [img_dict['img_pil'] for img_dict in images_list]
            contents = self.client.batch_two_step_extract(images=images_pil_list)
            results = []
            for page_info in contents:
                results.append(page_info)
            output.append([results])
        
        return output
    
    def close(self):
        del self.llm


class Convert2MDOp:
    
    def __init__(self, output_dir='./outputs_mineru', parse_method='vlm'):
        self.output_dir = output_dir
        self.parse_method = parse_method
    
    def run(self, model_results, images):

        output = []
        print(f"model_results length: {len(model_results)}, images length: {len(images)}")
        for idx in range(len(model_results)):
            model_result = model_results[idx][0]
            image = images[idx][0]
            pdf_file_name = Path(image[0]['pdf_path']).stem
            local_image_dir, local_md_dir = prepare_env(self.output_dir, pdf_file_name, self.parse_method)
            img_writer, md_writer = FileBasedDataWriter(local_image_dir), FileBasedDataWriter(local_md_dir)
            middle_json = result_to_middle_json(model_result, image, img_writer)

            # make md content string
            md_content_str = vlm_union_make(middle_json["pdf_info"], MakeMode.MM_MD, "images")
            md_writer.write_string(
                f"{pdf_file_name}.md",
                md_content_str,
            )
            # write content list json and markdown file
            content_list = vlm_union_make(middle_json["pdf_info"], MakeMode.CONTENT_LIST, "images")
            md_writer.write_string(
                f"{pdf_file_name}_content_list.json",
                json.dumps(content_list, ensure_ascii=False, indent=4),
            )
            # write middle json
            with open(os.path.join(local_md_dir, f"layout.json"), "w") as f:
                json.dump(middle_json, f, indent=4)
                
            output.append(f"{pdf_file_name}.md")
        return output