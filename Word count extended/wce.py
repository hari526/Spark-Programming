import sys
import re
from operator import add
from pyspark import SparkContext

def check(x):
    x = re.sub("--", '', x)
    x = re.sub("'", '', x)
    return re.sub('[?!@()]#$\'",.;:', '', x).lower().split(' ')

def storedata(x):
    return (len(x) > 0 or x != None)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print >> sys.stderr, "Usage: pyfile ipfile"
        exit(-1)
    sc = SparkContext(appName="most freq word")
    rdd = sc.textFile(sys.argv[1])
    rdd1 = rdd.flatMap(check).map(lambda x: (x, 1)).reduceByKey(add).filter(storedata)
    output = rdd1.map(lambda (k,v): (v,k)).sortByKey(False).take(2)
    for (count, word) in output:
        if (count<40000):
            print "%i: %s" % (count, word)