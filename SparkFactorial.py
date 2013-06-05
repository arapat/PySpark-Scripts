
import sys

from pyspark import SparkContext

def frac(n):
    t = 1
    for i in range(n):
        t = t * (i+1)
    return t

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print >> sys.stderr, \
                "Usage: sparkSpeedTest <master> <n>"
        sys.exit(1)

    sc = SparkContext(sys.argv[1], "SparkSpeedTest")
    nums = sc.parallelize(range(int(sys.argv[2])))
    print nums.map(frac).collect()
