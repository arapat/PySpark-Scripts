
import sys
import random

if __name__ == '__main__':
    if len(sys.argv) < 4:
        print >> sys.stderr, \
            "Usage: data_generator <dimension> <data_size> <random_seed>"
        sys.exit(1)

    d = int(sys.argv[1])
    total = int(sys.argv[2])
    seed = int(sys.argv[3])
    random.seed(seed)
    for i in range(total):
        data = [i+1] + [random.random() * 1000 for k in range(d)]
        print ' '.join([str(u) for u in data])

