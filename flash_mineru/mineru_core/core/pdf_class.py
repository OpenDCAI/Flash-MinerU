from typing import Optional, Union
from contextlib import contextmanager
import pypdfium2 as pdfium

class PdfDocument:
    def __init__(
            self, 
            source: Union[str, bytes],
            image_dir: str = "./images",
            markdown_dir: str = "./markdowns",
            start_page: int = 0,
            end_page: Optional[int] = None
        ):
        # 原始文件入口区域
        if isinstance(source, str):
            self._path = source
            self._bytes = None
        else:
            self._path = None
            self._bytes = source
        
        # image存放路径
        self.image_dir = image_dir
        self.markdown_dir = markdown_dir
        
        # 总页数
        self._n: Optional[int] = None
        # 待操作的页数
        self.page_start = start_page
        self.page_end = end_page
        assert self.page_start >= 0, "start_page must be non-negative"
        if self.page_end is not None:
            assert self.page_end > self.page_start, "end_page must be greater than start_page"

    def open_pdfium(self):
        return pdfium.PdfDocument(self._path) if self._path else pdfium.PdfDocument(self._bytes)
    
    @contextmanager
    def _open(self):
        """打开 PDF 文档并自动关闭。"""
        doc = self.open_pdfium()
        try:
            yield doc
        finally:
            doc.close()

    def __len__(self):
        if self._n is None:
            with self._open() as pdf:
                self._n = len(pdf)
        return self._n
