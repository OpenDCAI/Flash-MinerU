from multiprocessing import Process, Queue, connection


# 主控制类，不持有object，只当做所有PDF执行进度的控制器和看板
class ControlProcess(Process):
    pass

# Pipeline 1: PDF 转图片，纯CPU开销
class PDF2ImageProcess(Process):
    pass

# Pipeline 2: MinerU 图片识别，需要用GPU和大模型
class Mineru2_5RecognizeProcess(Process):
    pass

# Pipeline 3: MinerU 结果解析，合成markdown，纯CPU开销
class ResultParseProcess(Process):
    pass

# 主逻辑类
class FlashMineruMain:
    def __init__(self):
        # 进程间通过Queue通信 和 传递对象
        # object可能通过共享内存和UUID传递？
        self.control_process = ControlProcess()
        self.pdf2image_process = PDF2ImageProcess()
        self.mineru_recognize_process = Mineru2_5RecognizeProcess()
        self.result_parse_process = ResultParseProcess()



    # start 函数由主进程调用，启动所有进程，并启用队列监听
    def start(self):
        pass

    # cleanup 函数由主进程调用，清理所有子进程
    def cleanup(self):
        pass

    # run 函数仅由用户调用，传入 PDF 路径列表，启动处理
    def run(self, pdf_path_list):
        pass