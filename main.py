from map_reduce.framework import MapReduce
from jobs.word_count import word_count_mapper
from jobs.word_count import word_count_reducer


if __name__ == "__main__":
    input_text = [
        "Hello world",
        "Hello mapreduce bye mapreduce",
        "The sky is beautiful. The Sky is blue."
    ]

    print("----Input Data----")
    for line in input_text:
        print(line)
    
    print("-" * 20)

    map_reduce = MapReduce(mapper=word_count_mapper, reducer=word_count_reducer)
    output = map_reduce.execute(input_text)

    print("---- Final Output ----")
    for word, count in sorted(output):
        print(f"{word}: {count}")