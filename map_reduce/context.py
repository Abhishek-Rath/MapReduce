from collections import defaultdict

class JobContext:
    """
    Shared state of MapReduce job
    This class:
    - Stores the intermediate output of the mapper function
    - Stores the final output of the reduce function
    """

    def __init__(self):
        self.intermediate_data = defaultdict(list)
        self.result = []
    
    def _emit_intermediate(self, key, value):
        """Internal method to collect the output from the mapper"""
        self.intermediate_data[key].append(value)
    
    def _emit_result(self, record):
        """Internal method to collect the final output from the reducer"""
        self.result.append(record)
        
