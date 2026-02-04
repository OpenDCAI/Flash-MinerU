from flash_mineru import MineruEngine
import os, time

start_time = time.perf_counter()

engine = MineruEngine(
    model="<path_to_model>/MinerU2.5-2509-1.2B",
    batch_size=1, 
    replicas=3, 
    num_gpus_per_replica=1,
    save_dir="outputs_mineru",
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