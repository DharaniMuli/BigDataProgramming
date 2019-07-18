from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark import SparkContext, SparkConf


if __name__ == "__main__":


    sparkconfig = SparkSession.builder.appName("KSDataAnalysis").getOrCreate()
    sc = SparkContext(conf=sparkconfig)

    rdd1 = sc.read.format("csv").option("header", "true").load("ksdata.csv")

    # rdd1.select(rdd1.columns).distinct().show()
    # rdd1.select(rdd1['name']['category']).filter(rdd1['country']=='US').show()

lineLengths = rdd1.map(lambda s: len(s))
totalLength = lineLengths.reduce(lambda a, b: a + b)
 # query1 = rdd1.filter(rdd1['country']=='US').show()
    # Dataframe1 = rdd1.filter(rdd1['Country'] == 'US').show()