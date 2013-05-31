
# from pyspark import SparkContext
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
    A = WordCounts({'a': 1, 'b': 1, 'c': 1, 'd': 1})
    B = WordCounts({'a': 1, 'b': 1})
    print "A =", A
    print "B =", B
    print "A + B =", A+B
    print "KL(A,A) =", A.KLdiv(A)
    print "KL(B,A) =", B.KLdiv(A)
    print "KL(B,B) =", B.KLdiv(B)

    print "JS(A,A) =", A.JSdiv(A)
    print "JS(A,B) =", A.JSdiv(B)
    print "JS(B,A) =", B.JSdiv(A)

    print "Next line is supposed to throw an exception because of division by zero"
    print "KL(A,B) =", A.KLdiv(B)
