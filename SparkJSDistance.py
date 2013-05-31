"""
Expect the input file consists of lines of (word, count)+ 
Example:
    Spark 10 is 14 so 21 cool 17
    Spark 20 is 25 a 10 good 27 tool 10 for 42 data 13 analysis 9
    PySpark 3 is 30 the 43 Python 8 interface 10 of 52 Spark 13
"""


from pyspark import SparkContext
from math import log

class WordCounts:

    def __init__(self, wordcounts):
        # A map in form of ("word": count)
        self.wordcounts = wordcounts
        self.keys = wordcounts.keys()
        self.total = sum(wordcounts.values())
        
        if self.total == 0:
            raise Exception("Could not initialize empty WordCounts instance.")
        self.fraction = {word: float(count)/float(self.total) for word,count in wordcounts.items()}


    def __add__(self, that):
        sumdict = {}
        for key in self.keys + that.keys:
            sumdict[key] = self.wordcounts.get(key, 0) + that.wordcounts.get(key, 0)
        return WordCounts(sumdict)


    def __radd__(self, that):
        return self + that

    def __getitem__(self, key):
        """IMPORTANT: by default, WordCounts[key] will return the percentage but not the exact counts."""
        return self.fraction.get(key, 0.0)


    def KLdiv(self, that):
        """Compute KL divergence of self.wordcounts and that.wordcounts.
        self.wordcounts.keys must be subset of that.wordcounts.key"""
        if any(key not in that.keys for key in self.keys):
            raise Exception("KL-divergence could only be compute with its superset.")

        return sum( \
                self[key] * log(float(self[key])/float(that[key]), 2) \
                for key in self.keys)

    def JSdiv(self, that):
        mean = self + that
        return self.KLdiv(mean) + that.KLdiv(mean)
        

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print >> sys.stderr, \
                "Usage: PythonJSDistance <master> <word_count file>"
        exit(-1)

    data = []
    for dist in sys.stdin:
        res = {}
        dist = dist.split()
        for idx in range(0, len(dist), 2):
            res[dist[idx]] = int(dist[idx+1])

        data.append(res)

        

    pair = []
    for a in data:
        for b in data:
            if a != b:

