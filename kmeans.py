"""
Revised based on PySpark example (https://github.com/mesos/spark/tree/master/python/examples)
This example requires numpy (http://www.numpy.org/)
"""
import sys

import numpy as np
from pyspark import SparkContext


def parseVector(line):
    return np.array([float(x) for x in line.split(' ')])


# Return the index of the closest center to p
def closestPoint(p, centers):
    bestIndex = 0
    closest = float("+inf")
    for i in range(len(centers)):
        tempDist = np.sum((p - centers[i]) ** 2)
        if tempDist < closest:
            closest = tempDist
            bestIndex = i
    return bestIndex


if __name__ == "__main__":
    if len(sys.argv) < 5:
        print >> sys.stderr, \
            "Usage: PythonKMeans <master> <file> <k> <convergeDist>"
        exit(-1)

    minChanges = 0

    sc = SparkContext(sys.argv[1], "PythonKMeans")
    lines = sc.textFile(sys.argv[2])
    data = lines.map(parseVector).cache()
    K = int(sys.argv[3])
    convergeDist = float(sys.argv[4])

    # TODO: change this after we port takeSample()
    #kPoints = data.takeSample(False, K, 34)
    # Take first K elements
    kPoints = data.take(K)
    tempDist = 1.0

    changes = minChanges + 1
    prevIndex = [i for i in range(data.count())]
    while tempDist > convergeDist:
    #while changes > minChanges:
        # Return the tuple of (Closest_center, (data_point, 1))
        closest = data.map(
            lambda p : (closestPoint(p, kPoints), (p, 1)))
        # Reduce closest by key
        pointStats = closest.reduceByKey(
            lambda (x1, y1), (x2, y2): (x1 + x2, y1 + y2))
        # collect() will return a list from RDD
        newPoints = pointStats.map(
            lambda (x, (y, z)): (x, y / z)).collect()

        closestList = closest.collect()

        newPointsMap = {x: y for (x, y) in newPoints}
        curIndex = [x for (x, y) in closestList]

        # Center moves
        tempDist = sum(np.sum((kPoints[x] - y) ** 2) for (x, y) in newPoints)
        # Average distance from each data point to the closest center
        avgDist = sum(np.sum((newPointsMap[x] - y[0]) ** 2) for (x, y) in closestList)
        changes = 0
        for idx, val in enumerate(prevIndex):
            if val != curIndex[idx]:
                changes += 1
        prevIndex = curIndex

        print '[LOG]', 'Centers move:', tempDist, '\tAverage distance:', avgDist, '\tcenter changes:', changes

        for (x, y) in newPoints:
            kPoints[x] = y

    print "Final centers: " + str(kPoints)

