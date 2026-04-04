from flash_mineru import MineruEngine
import os, time

start_time = time.perf_counter()

engine = MineruEngine(
    model="<path_to_local>/MinerU2.5-2509-1.2B",
    # Model: https://huggingface.co/opendatalab/MinerU2.5-2509-1.2B
    batch_size=2,  # PDFs per logical batch; often choose a multiple of GPU count
    replicas=3,  # Parallel vLLM / model instances; often match GPU count
    num_gpus_per_replica=0.9,  # GPU memory fraction for vLLM KV cache per instance; 1.0 uses full VRAM headroom
    save_dir="outputs_mineru",
    inflight=4,  # Pipeline depth (v1.0.0 path); can raise on high-memory hosts with diminishing returns
)
data = []
data_dir = "test/sample_pdfs"

for file in os.listdir(data_dir):
    data.append(os.path.join(data_dir, file))
    
results = engine.run(data)

print("Final Result:")
# results is a list of list
print(results)

end_time = time.perf_counter()
print(f"Total time taken: {end_time - start_time} seconds")
