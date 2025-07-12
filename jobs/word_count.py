def word_count_mapper(line):
    """
    Mapper function for word count
    """
    if not isinstance(line, str):
        return
    words = line.strip().lower().split()
    for word in words:
        yield(word, 1)


def word_count_reducer(word, counts):
    """
    Reducer function for word count
    """
    count_of_word = sum(counts)
    return (word, count_of_word)