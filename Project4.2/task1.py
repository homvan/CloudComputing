import sys
import pyspark

file = spark.textFile("hdfs:///input")
edgeCounts = file.map(lambda line: (line, 1)) \
    .reduceByKey(lambda a, b: a + b)
vertexCounts = file.flatMap(lambda line: line.split(" ")) \
             .map(lambda node: (node, 1)) \
             .reduceByKey(lambda a, b: a + b)
edgeCounts.saveAsTextFile("hdfs:///edgeoutput")
vertexCounts.saveAsTextFile("hdfs:///vertexoutput")
