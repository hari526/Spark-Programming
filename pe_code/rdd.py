import sys
from pyspark import SparkContext
from operator import add


if len(sys.argv) != 4:
        print >> sys.stderr, "command: pyfile ipfile1 ipfile2 opfile"
        exit(-1)
        
sc = SparkContext(appName="rdd_join")
text_file1 = sc.textFile(sys.argv[1])
rdd1 = text_file1.filter(lambda line: "Spark" in line)
rdd1.cache()

text_file2 = sc.textFile(sys.argv[2])
rdd2 = text_file2.filter(lambda line: "Spark" in line)
rdd2.cache()

file1 = rdd1.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(add)
file2 = rdd2.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(add)

op=file1.join(file2)
op.saveAsTextFile(sys.argv[3])