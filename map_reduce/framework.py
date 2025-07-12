import collections
import time

class MapReduce:
    def __init__(self, mapper, reducer):
        """
        Initializes the MapReduce job
        """
        self.mapper = mapper
        self.reducer = reducer
        self.intermediate_data = collections.defaultdict(list)
        self.result = []
        self.timings = {}
    
    def _emit_intermediate(self, key, value):
        """Internal method to collect the output from the mapper"""
        self.intermediate_data[key].append(value)
    
    def _emit_result(self, record):
        """Internal method to collect the final output from the reducer"""
        self.result.append(record)

    def execute(self, input_data):
        """Executes the Map Reduce job"""
        print("-----Phase: MAP----")
        map_start_time = time.perf_counter()
        for record in input_data:
            for key, value in self.mapper(record):
                self._emit_intermediate(key, value)
        map_end_time = time.perf_counter()
        self.timings['map'] = map_end_time - map_start_time
        print(f"Intermediate data has {len(self.intermediate_data)} unique keys")
        print(f"Map phase took: {self.timings['map']:.4f} seconds.")

        print("\n----PHASE: REDUCE----")
        reduce_start_time = time.perf_counter()
        sorted_keys = sorted(self.intermediate_data.keys())
        for key in sorted_keys:
            values = self.intermediate_data[key]
            output_record = self.reducer(key, values)
            if output_record:
                self._emit_result(output_record)
        reduce_end_time = time.perf_counter()
        self.timings['reduce'] = reduce_end_time - reduce_start_time
        print(f"Reduce phase took: {self.timings['reduce']:.4f} seconds.")
        
        print("----JOB COMPLETE----")
        total_time = self.timings['map'] + self.timings['reduce']
        print(f"Total execution time: {total_time:.4f} seconds.")
        return self.result