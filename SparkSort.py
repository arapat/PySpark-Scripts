"""
Merge sort using PySpark API
"""

import types 

from pyspark import SparkContext

def merge(item, prev):
    if not isinstance(item, types.ListType):
        item = [item]

    union = []
    iteru = iter(item)
    iterv = iter(prev)

    u = None
    v = None
    try:
        u = iteru.next()
    except StopIteration:
        pass
    try:
        v = iterv.next()
    except StopIteration:
        pass

    try:
        while u and v:
            if u < v:
                print 'appending',u
                union.append(u)
                u = None
                u = iteru.next()
            else:
                print 'appending',v
                union.append(v)
                v = None
                v = iterv.next()
    except StopIteration:
        pass

    rest = iter([])
    if u is not None:
        union.append(u)
        rest = iteru
    elif v is not None:
        union.append(v)
        rest = iterv

    try:
        while True:
            union.append(rest.next())
    except StopIteration:
        pass

    return union


if __name__ == '__main__':
    sc = SparkContext('local', 'SparkSort')
    data = sc.parallelize(range(100,0,-1))
    print data.fold([], merge)
