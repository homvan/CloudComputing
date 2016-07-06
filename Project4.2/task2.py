import sys
from pyspark import SparkContext
from operator import add

if __name__ == "__main__":

    sc = SparkContext(appName="task2")
    file = sc.textFile("hdfs:///input")
    counts = file.flatMap(lambda line: line.split(' ')[1]) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)
    output = counts.collect()
    formatOutput = ''
    for (word, count) in output:
        formatOutput += ("%s\t%i\n" % (word, count))
    formatOutput.saveAsTextFile("hdfs:///formatOutput")
    sc.stop()