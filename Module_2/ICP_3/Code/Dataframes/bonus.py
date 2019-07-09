import pyspark
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import *

from pyspark.sql import SQLContext
from pyspark.sql.functions import udf

def dummy_function(data_str):
    cleaned_str = 'dummyData'
    return cleaned_str

dummy_function_udf = udf(dummy_function, StringType())

sc = SparkSession.builder.appName("SurveyAnalysis").getOrCreate()

# Load a text file and convert each line to a Row.
lines = sc.read.text("demo.txt")
parts = lines.map(lambda l: l.split(","))
training = parts.map(lambda p: (p[0], p[1]))

# Create dataframe
training_df = sc.createDataFrame(training, ["msg", "con"])
training_df.show()

import pyspark
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf

sc = SparkSession.builder.appName("SurveyAnalysis").getOrCreate()


training_df = sc.sql("select msg as tweet, 'bar' as classification")

def dummy_function(data_str):
     cleaned_str = 'dummyData'
     return cleaned_str

dummy_function_udf = udf(dummy_function, StringType())
df = training_df.withColumn("dummy", dummy_function_udf(training_df['tweet']))
df.show()