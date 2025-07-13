import time

from .task import MapTask, ReduceTask
from .worker import Worker
from .context import JobContext 


class Master:
    """
    The Master class coordinates the execution of the Mapreduce job
    """

    def __init__(self, input_data, mapper, reducer, num_workers=3):
        self.input_data = input_data
        self.mapper = mapper
        self.reducer = reducer
        self.num_workers = num_workers
        self.context = JobContext()
        self.timings = {}
    
    def execute(self):
        """Executes the Map Reduce job"""
        print("-----Phase: MAP----")
        map_start_time = time.perf_counter()
        
        map_tasks = [MapTask(i, line) for i, line in enumerate(self.input_data)]
        print("Map tasks: ", map_tasks)

        workers = [Worker(i) for i in range(self.num_workers)]
        print("Workers: ", workers)

        for i, task in enumerate(map_tasks):
            worker = workers[i % self.num_workers]
            worker.run_map(task, self.mapper, self.context)

        map_end_time = time.perf_counter()
        self.timings['map'] = map_end_time - map_start_time
        print(f"Intermediate data has {len(self.context.intermediate_data)} unique keys")
        print(f"Map phase took: {self.timings['map']:.4f} seconds.")

        print("\n----PHASE: REDUCE----")
        reduce_start_time = time.perf_counter()
        
        reduce_tasks = [
            ReduceTask(i, key, values)
            for i, (key, values) in enumerate(sorted(self.context.intermediate_data.items()))
        ]
        print("reduce tasks: ", reduce_tasks)


        for i, task in enumerate(reduce_tasks):
            worker = workers[i % self.num_workers]
            worker.run_reduce(task, self.reducer, self.context)

        reduce_end_time = time.perf_counter()
        self.timings['reduce'] = reduce_end_time - reduce_start_time
        print(f"Reduce phase took: {self.timings['reduce']:.4f} seconds.")
        
        print("----JOB COMPLETE----")
        total_time = self.timings['map'] + self.timings['reduce']
        print(f"Total execution time: {total_time:.4f} seconds.")
        return self.context.result