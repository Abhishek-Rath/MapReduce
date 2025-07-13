from map_reduce.master import Master
from jobs.word_count import word_count_mapper as mapper
from jobs.word_count import word_count_reducer as reducer


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

    job = Master(input_text, mapper, reducer, num_workers=2)
    output = job.execute()

    print("\n--- Final Output ---")
    for word, count in sorted(output):
        print(f"{word}: {count}")