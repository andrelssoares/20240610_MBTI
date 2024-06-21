import csv
import re
import pandas

from mrjob.job import MRJob
from mrjob.step import MRStep
from pandas import DataFrame

df = DataFrame('mbti.csv')

class MBTICount(MRJob):
    def mapper(self, key, value):
        mbti_type = df['type']
        tokens = re.findall(r"\b\w+\b", mbti_type.lower())
        for token in tokens:
            yield token, 1

    def combiner(self, key, values):
        yield key, sum(values)

    def reducer(self, key, values):
        yield None, (sum(values), key)

    def reducer_sorter(self, key, values):
        for count, key in sorted(values):
            yield count, key

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                reducer=self.reducer
            ),
            MRStep(
                reducer=self.reducer_sorter
            )
        ]
if __name__ == '__main__':
    MBTICount.run()