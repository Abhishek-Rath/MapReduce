from map_reduce.task import MapTask, ReduceTask

def test_map_task_fields():
    task = MapTask(task_id=1, data_chunk="I am the half blood prince")
    assert task.task_id == 1
    assert task.data_chunk == "I am the half blood prince"

def test_reduce_task_fields():
    task = ReduceTask(task_id=2, key="word", values=[1, 1, 1])
    assert task.task_id == 2
    assert task.key == "word"
    assert task.values == [1, 1, 1]
