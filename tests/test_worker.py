from map_reduce.worker import Worker
from map_reduce.task import MapTask, ReduceTask
from map_reduce.context import JobContext

def dummy_mapper(line):
    for word in line.strip().split():
        yield (word.lower(), 1)

def dummy_reducer(key, values):
    return (key, sum(values))

def test_worker_map():
    context = JobContext()
    task = MapTask(0, "hello world hello")
    worker = Worker(worker_id=0)

    worker.run_map(task, dummy_mapper, context)

    assert context.intermediate_data["hello"] == [1, 1]
    assert context.intermediate_data["world"] == [1]

def test_worker_reduce():
    context = JobContext()
    task = ReduceTask(0, "hello", [1, 1, 1])
    worker = Worker(worker_id=0)

    worker.run_reduce(task, dummy_reducer, context)

    assert context.result == [("hello", 3)]
