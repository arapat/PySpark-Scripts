
import sys
from pyspark import SparkContext
from math import log

class WordCounts:

    def __init__(self, wordcounts, index):
        # A map in form of ("word": count)
        self.index = index
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
                "Usage: pySparkJSDist <master> <file>"
        sys.exit(1)

    # Process the input data
    dist = []
    input_file = open(sys.argv[2])
    counter = 0
    for row in input_file:
        counter = counter + 1
        row = row.split()
        dist.append( \
                WordCounts({ row[idx]: int(row[idx + 1]) \
                for idx in range(0,len(row),2)}, counter) )
    input_file.close()

    # Create Spark context
    sc = SparkContext(sys.argv[1], "pySparkJSDist")
    dist_rdd = sc.parallelize(dist).cache()

    # Compute the JS-divergence between each distribution and "one"
    answer = []
    for one in dist:
        JSdiv = lambda x: (one.index, x.index, one.JSdiv(x))
        answer.append(dist_rdd.map(JSdiv).collect())

    for item in answer:
        print "(%d, %d) = %f" % (item[0], item[1], item[2])

