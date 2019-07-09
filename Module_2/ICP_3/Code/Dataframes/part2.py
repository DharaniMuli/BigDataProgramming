from pyspark.sql import SparkSession
from pyspark.sql import *

if __name__ == "__main__":
    spark = SparkSession.builder.appName("SurveyAnalysis").getOrCreate()

    smalldf = spark.read.format("csv").option("header", "true").load("df1.csv")

    smalldf.createOrReplaceTempView("Survey")

    # df1 = spark.sql("Select Sno, timestamp, Age, Country from (select ROW_NUMBER() over (order By timestamp) as Sno, * from Survey) a where Sno=2")
    # df1.show()

    # df2 = spark.sql(
    #     "Select rownumber,Country,age from (select ROW_NUMBER() OVER (ORDER BY timestamp ) AS rownumber, * from Survey) a"
    #     " where Country = 'United States' ")
    # df3 = spark.sql(
    #     "Select rownumber,country,state from (select ROW_NUMBER() OVER (ORDER BY timestamp ) AS rownumber, * from Survey) a"
    #     " where Country = 'United States' ")
    #
    # df2.join(df3, df2["rownumber"] == df3["rownumber"]).show()
    #
    df5 = spark.sql(
        "Select rownumber,timestamp,age from (select ROW_NUMBER() OVER (ORDER BY timestamp ) AS rownumber, * from survey) a"
        " where Age >= 30 ")
    df6 = spark.sql(
        "Select rownumber,country,state from (select ROW_NUMBER() OVER (ORDER BY timestamp ) AS rownumber, * from survey) a"
        " where Age >= 40  ")

    df5.join(df6, df5["rownumber"] == df6["rownumber"]).show()

    df6 = spark.sql(
        "Select Avg(age) as AverageAge from Survey").show()

    df7 =spark.sql("Select count(*) as Total from Survey where country='Canada'").show()