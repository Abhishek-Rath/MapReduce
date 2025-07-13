class MapTask:
    """
    Represents a unit map of work
    """
    def __init__(self, task_id, data_chunk):
        self.task_id = task_id
        self.data_chunk = data_chunk

class ReduceTask:
    """
    Represents a unit of reduce work: one key + its associated values.
    """
    def __init__(self, task_id, key, values):
        self.task_id = task_id
        self.key = key 
        self.values = values
