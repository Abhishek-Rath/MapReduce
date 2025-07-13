from map_reduce.context import JobContext

def test_emit_intermediate():
    context = JobContext()
    context._emit_intermediate("hello", 1)
    context._emit_intermediate("hello", 1)
    context._emit_intermediate("world", 1)

    assert context.intermediate_data["hello"] == [1, 1]
    assert context.intermediate_data["world"] == [1]

def test_emit_output():
    context = JobContext()
    context._emit_result(("hello", 2))
    context._emit_result(("world", 1))

    assert context.result == [("hello", 2), ("world", 1)]
