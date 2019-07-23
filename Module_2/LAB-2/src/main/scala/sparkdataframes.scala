import org.apache.avro.generic.GenericData.StringType
import org.apache.spark._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf



object sparkdataframes {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("SparkDataFrames")
      .master("local")
      .getOrCreate()
    val df = spark.read.format("csv").option("header", "true").load(".\\ksdata.csv")

    //    df.show()

    //    df.filter(df("category ") === "Poetry").show()

    //    df.groupBy(df("name ")).count().show()

    //df.select(df("backers ")).distinct().show()

    df.select("currency ", "country ").filter(df("country ")==="US").show()


    def udfUppercase = udf((string: String) => string.toLowerCase())

    // Convert a whole column to uppercase with a UDF.
  df.withColumn("currency ", udfUppercase(df("currency "))).show()

  }
}