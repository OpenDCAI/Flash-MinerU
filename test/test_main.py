from flash_mineru import MineruEngine
import os, time

start_time = time.perf_counter()
engine = MineruEngine(batch_size=64, replicas=3, num_gpus_per_replica=0.25)
data = []

for file in os.listdir("sample_pdfs"):
    data.append(os.path.abspath(os.path.join("sample_pdfs", file)))
    
result = engine.run(data)

print(result)

end_time = time.perf_counter()

print(f"Total time taken: {end_time - start_time} seconds")