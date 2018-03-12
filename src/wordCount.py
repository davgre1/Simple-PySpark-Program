import operator
import sys


# Check spark modules
try:
    from pyspark import SparkContext, SparkConf
    print("Finished importing Spark Modules")

except ImportError as e:
    print("Error importing Spark Modules", e)
    sys.exit(1)

if __name__ == "__main__":
    conf = SparkConf().setMaster("local")\
            .setAppName("spark_wc")
    sc = SparkContext(conf=conf)

    # log4j = sc._jvm.org.apache.log4j
    # log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)

    # Read in file
    contentRDD = sc.textFile("C:\\Workspace\\wordcount\\input.txt")

    words = contentRDD.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(operator.add).collect()

    # Output results
    for w in words:
        print(w)

    # Save file
    # count.saveAsTextFile("C:\\Workspace\\wordcount\\output.txt")
