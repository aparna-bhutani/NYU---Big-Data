import sys
from pyspark import SparkContext
import csv


if __name__=='__main__':

    sc = SparkContext()
    file1 = sys.argv[1]
    out = sys.argv[2]

    data = sc.textFile(file1).cache()
    header = data.take(1)
    data = data.filter(lambda r: r not in header) \
            .mapPartitions(lambda l: csv.reader(l, delimiter=',', quotechar='"')) \
            .filter(lambda x: len(x) > 7 and type(x[0]) == str) \
            .filter(lambda x: len(x[0]) == 10) \
            .map(lambda y: ((y[1], y[0][:4], y[7].lower()),1)) \
            .reduceByKey(lambda a,b: a + b) \
            .map(lambda el: ((el[0][0], el[0][1]), (el[0][2], el[1]))) \
            .reduceByKey(lambda a,b: a + b) \
            .map(lambda e: (e[0][0], e[0][1], sum(i for i in e[1][1::2]),
                                len(e[1][::2]),
                                round(100*max(i for i in e[1][1::2])/sum(i for i in e[1][1::2]))
                           )) \
            .sortBy(lambda el: (el[0][0],el[0][1])) \
            .saveAsTextFile(out)
