"""
Input file should consist of lines of distributions.
Example: jsdistance.txt

Output: the value of JS divergence for each pairs of distributes.
"""

import sys
from pyspark import SparkContext
from math import log

from SparkJSDistUtil import WordCounts


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
    sc = SparkContext(sys.argv[1], "pySparkJSDist", pyFiles=['SparkJSDistUtil.py'])
    dist_rdd = sc.parallelize(dist).cache()

    # Compute the JS-divergence between each distribution and "one"
    answer = []
    for one in dist:
        JSdiv = lambda x: (one.index, x.index, one.JSdiv(x))
        answer = answer + dist_rdd.map(JSdiv).collect()

    for item in answer:
        print "(%d, %d) = %f" % (item[0], item[1], item[2])

