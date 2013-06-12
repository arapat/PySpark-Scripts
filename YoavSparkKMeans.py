"""
Revised based on PySpark example (https://github.com/mesos/spark/tree/master/python/examples)
This example requires numpy (http://www.numpy.org/)
"""
import sys
from operator import add
from math import log
from random import random

import numpy as np
from pyspark import SparkContext


def updateHash(hist, cur):
    """
        Encode the partition number *cur* into a two-digit 95-based number,
        each digit is representing with ascii codes between 32-126 (visible characters).
        *cur* should be in range of [0, 9025) (thus K <= 9025).
        Return hist + encoded(cur)
    """
    hx = chr(cur/95 + 32) + chr(cur%95 + 32)
    return hist + hx


def parseVector(line):
    """
        Return (index, coordinates)
    """
    index, vector = line.split(' ', 1)
    return int(index), np.array([float(x) for x in vector.split(' ')])


def entropy(assoc, data_size):
    data_size = float(data_size)
    # Reduce the association information to (value, count) pairs
    assign = assoc.map(lambda (index, parti): (parti, 1)).reduceByKey(add)
    # Compute the distribution
    dist = assign.map(lambda (x, y): y/data_size)
    # Compute the entropy of the distribution
    entropy = -dist.map(lambda u: -u*log(u,2)).reduce(add)
    count = dist.count()
    return entropy, count


def closestPoint(p, centers):
    """
    Return the index of p and the closest center to p as a tuple.
    """
    index = p[0]
    vector = p[1]

    bestIndex = 0
    closest = float("+inf")
    for i in range(len(centers)):
        tempDist = np.sum((vector - centers[i]) ** 2)
        if tempDist < closest:
            closest = tempDist
            bestIndex = i
    return index, bestIndex


if __name__ == "__main__":
    if len(sys.argv) < 5:
        print >> sys.stderr, \
            "Usage: PythonKMeans <master> <file> <k> <convergeDist>"
        exit(-1)

    sc = SparkContext(sys.argv[1], "PythonKMeans")
    lines = sc.textFile(sys.argv[2])
    data = lines.map(parseVector).cache()
    data_size = data.count()
    K = int(sys.argv[3])
    convergeDist = float(sys.argv[4])

    if K >= data_size:
        print >> sys.stderr, \
            "Error: Larger K than the data size. K-means always equal to 0 in this case."
        exit(-1)

    if K > 9025:
        # See function updateHash for explanation
        print >> sys.stderr, \
            "Error: K is too large. K should be no larger than 9025."
        exit(-1)


    def takeSample(K, data_size, filename):
        """
            Simple takeSample implementation.
            PySpark does not support takeSample transformation yet.
        """
        ratio = K / float(data_size)
        sample = []
        picked = 0
        remain = data_size

        f = open(filename)
        for line in f:
            if picked + remain <= K or random() <= ratio:
                sample.append(parseVector(line)[1])

                picked += 1
                if picked >= K:
                    break
            remain -= 1
        f.close()

        return sample


    def Kmeans(kPoints):
        """
            The closure "Kmeans" is to
            compute K-means as usual except the distance function
            is computing their JS-divergence

            Output: (data_point_index, closest_center) for all points
        """
        # Initial settings
        tempDist = 1.0
        minChanges = 0
        changes = minChanges + 1
        prevIndex = [i for i in range(data_size)]

        while tempDist > convergeDist:
        #while changes > minChanges:
            # Return the tuple of (closest_center, (data_point_coordinates, 1))
            closest = data.map(
                lambda p : (closestPoint(p, kPoints)[1], (p[1], 1)))
            # Reduce closest by key
            pointStats = closest.reduceByKey(
                lambda (x1, y1), (x2, y2): (x1 + x2, y1 + y2))
            # collect() will return a list from RDD
            # Return (prev_closest_center, new_closest_center_coordinates)
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

            print >> sys.stderr, \
                '[LOG]', 'Centers move:', tempDist, '\tAverage distance:', avgDist, '\tcenter changes:', changes

            for (x, y) in newPoints:
                kPoints[x] = y

        return data.map(
                lambda p: closestPoint(p, kPoints))

    
    """
        Main Process:
        Compute the entropy of the K-means partition with
        different initial centers, as well as the entropy
        of the combined partitions
    """
    partitions = sc.parallelize([(i, '') for i in range(1, data_size+1)])
    for i in range(3):
        # Take K elements randomly
        # TODO: change this after PySpark port takeSample()
        kPoints = takeSample(K, data_size, sys.argv[2])

        partition = Kmeans(kPoints)
        partitions = partitions.join(partition).map(lambda (index, (prev, p)): (index, updateHash(prev, p)))
        e1 = entropy(partition, data_size)
        e_total = entropy(partitions, data_size)
        print "=========================="
        print "seed:", kPoints
        print "singleton partition =", e1
        print "combined partition =", e_total

