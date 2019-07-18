from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark import SparkContext


if __name__ == "__main__":

    sc = SparkContext()
    sqlContext = SQLContext(sc)

    df = sqlContext.read.csv("ksdata.csv")
    # df.show()
    # df.printSchema()

    #Groupby main Categories
    df.select("_c9").groupBy("_c9").count().show()

    #Filtering data whose state is successfull
    # df.select(df['_c1'],df['_c2'],df['_c9']).filter(df['_c9'] == 'successful').show()