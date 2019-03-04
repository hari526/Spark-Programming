import sys
from pyspark import SparkContext
from pyspark.mllib.classification import LogisticRegressionWithSGD
from pyspark.mllib.regression import LabeledPoint

def data_in(line):
    text = [float(s) for s in line.split(' ')]
    if text[0] == -1:   # Convert -1 labels to 0 for MLlib
        text[0] = 0
    return LabeledPoint(text[0], text[1:])


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print >> sys.stderr,"Command: log_reg_py file ip_file iteration"
        exit(-1)

    sc = SparkContext(appName="log_reg")
    iteration = int(sys.argv[2])
    rdd = sc.textFile(sys.argv[1])
    rdd1 = rdd.map(data_in)
    
    for m in range(iteration):
     	model = LogisticRegressionWithSGD.train(rdd1,iteration)
    print("final weights :" + str(model.weights))