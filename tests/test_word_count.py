from map_reduce.framework import MapReduce
from jobs.word_count import word_count_mapper, word_count_reducer

def test_word_count_logic():
    """
    Tests the end-to-end logic of the word count MapReduce job.
    """

    test_input = [
        "Hello world",
        "Hello test"
    ]

    expected_output = {
        "hello": 2,
        "world": 1,
        "test": 1
    }

    word_counter = MapReduce(word_count_mapper, word_count_reducer)
    results = word_counter.execute(test_input)

    output_dict = dict(results)
    assert output_dict == expected_output
    print("\n[Test Passed]: test_word_count_logic passed successfully.")