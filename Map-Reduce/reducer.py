#!/home/shivam_gupta/Documents/pip3-venv python
"""A more advanced Reducer, using Python iterators and generators."""

from itertools import groupby
from operator import itemgetter
import sys
import csv

def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.strip().split(separator, 1)

def main(separator='\t'):
    # input comes from STDIN (standard input)
    data = read_mapper_output(sys.stdin, separator=separator)
    # convert count (currently a string) to int
    word2count = {}
    for current_word, count in data:
        #print(current_word,count)
        try:
            count = int(count)
            word2count[current_word] = word2count.get(current_word, 0) + count
        except ValueError:
            # count was not a number, so silently
            # ignore/discard this line
            pass
    # sort the words lexigraphically;
    # this step is NOT required, we just do it so that our
    # final output will look more like the official Hadoop
    # word count examples
    sorted_word2count = sorted(word2count.items(), key=itemgetter(1),reverse=True)
    # write the results to STDOUT (standard output)
    output = csv.writer(open("wordcount.csv", "w"))
    output.writerow(('Words', 'Counts'))
    for word, count in sorted_word2count:
        output.writerow([word, count])


if __name__ == "__main__":
    main()