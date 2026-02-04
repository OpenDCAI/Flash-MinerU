from flash_mineru.ray_utils import (
    RayModule,
    Dispatch,
)

from flash_mineru.mineru_core.dispatch_mineru_class import (
    Convert2MDOp,
    Pdf2ImageOp,
    ProcessImagesOp,
)

from typing import Optional

class MinerUPipeline():

    def __init__(
        self,
        *,
        replicas: int = 1,
        num_gpus_per_replica: float = 1.0,
        model: str = "/home/dataset-local/models/MinerU2.5-2509-1.2B",
        device: str = "cuda",
        save_dir: str = "outputs_mineru",
        meta_json_path: str = "outputs/meta_data.json",
    ):
        self.save_dir = save_dir
        self.meta_json_path = meta_json_path

        self.pdf2img = RayModule(
            Pdf2ImageOp,
            replicas=1,
            num_gpus_per_replica=0.0,
        ).pre_init()

        self.process_img = RayModule( 
            ProcessImagesOp,
            replicas=replicas,
            num_gpus_per_replica=num_gpus_per_replica if device.startswith("cuda") else 0.0,
            dispatch_mode=Dispatch.ALL_SLICED_TO_ALL,
        ).pre_init(model=model, gpu_memory_utilization=num_gpus_per_replica if device.startswith("cuda") else 0.0)

        self.img2md = RayModule(
            Convert2MDOp,
            replicas=1,
            num_gpus_per_replica=0.0,
            dispatch_mode=Dispatch.ALL_SLICED_TO_ALL,
        ).pre_init(output_dir=self.save_dir, parse_method='vlm')
    

    def run(self, image_paths: list[str]):
        print("Running MinerU Pipeline on data:", image_paths)
        
        images = self.pdf2img(image_paths)
        model_results = self.process_img(images)
        ans = self.img2md(model_results=model_results, images=images)
        
        return ans

class MineruEngine():
    def __init__(
        self,
        *,
        model: str = "/home/dataset-local/models/MinerU2.5-2509-1.2B",
        device: str = "cuda",
        save_dir: str = "outputs_mineru",
        meta_json_path: str = "outputs/meta_data.json",
        batch_size: int = 4,
        replicas: Optional[int] = None,
        num_gpus_per_replica: Optional[float] = None,
    ):
        import torch
        gpu_count = torch.cuda.device_count()
        
        assert (replicas is None or num_gpus_per_replica is not None), "Please specify num_gpus_per_replica when replicas is given."
        assert (replicas is None or num_gpus_per_replica * replicas <= gpu_count), f"Too many gpus requested: num_gpus_per_replica * replicas = {num_gpus_per_replica * replicas}, max allowed: {gpu_count}"
        
        self.pipeline = MinerUPipeline(
            model=model,
            device=device,
            save_dir=save_dir,
            meta_json_path=meta_json_path,
            replicas=replicas if replicas is not None else 4,
            num_gpus_per_replica=num_gpus_per_replica if replicas is not None else 0.2,
        )
        self.batch_size = batch_size

    def run(self, path_of_pdfs: list[str]):

        print("MineruEngine is running... for ", path_of_pdfs)
        steps = (len(path_of_pdfs) + self.batch_size - 1) // self.batch_size
        results = []
        for step in range(steps):
            batch_paths = path_of_pdfs[step * self.batch_size : (step + 1) * self.batch_size]
            result = self.pipeline.run(batch_paths)
            results.append(result)
            print(f"MineruEngine finished batch {step + 1}/{steps}")
            
        print("MineruEngine finished.")

        return results