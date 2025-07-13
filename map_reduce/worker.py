class Worker:
    """
    This class represents the worker node.
    Can execute either map or reduce tsaks
    """

    def __init__(self, worker_id):
        self.worker_id = worker_id
    
    def run_map(self, task, mapper, context):
        print(f"[Worker {self.worker_id}] Running MapTask {task.task_id}")
        for key, value in mapper(task.data_chunk):
            context._emit_intermediate(key, value)
    
    def run_reduce(self, task, reducer, context):
        print(f"[Worker {self.worker_id}] Running ReduceTask {task.task_id} for key '{task.key}'")
        result = reducer(task.key, task.values)
        if result:
            context._emit_result(result)