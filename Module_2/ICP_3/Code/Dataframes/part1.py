from pyspark.sql import SparkSession
from pyspark.sql import *


if __name__ == "__main__":


    spark = SparkSession.builder.appName("SurveyAnalysis").getOrCreate()

    # Import the dataset and create data frames directly on import.
    # df = spark.read.format("csv").option("header", "true").load("survey.csv")
    #
    #
    #
    # # Save imported data to file
    # df.write.csv('SurveyAnalysisOutput.csv');

    smalldf = spark.read.format("csv").option("header", "true").load("df1.csv")

    # Check for Duplicate records from the dataset
    # smalldf.select(smalldf.columns).distinct().show()


    Dataframe1 = smalldf.filter(smalldf['Country'] == 'Canada').show()

    Dataframe2 = smalldf.filter(smalldf['Country'] == 'United States')

    # UnionOperation = Dataframe1.union(Dataframe2).orderBy(smalldf['Gender']).show()

    # groupOperation = Dataframe1.union(Dataframe2).groupBy(smalldf['treatment']).count().show()