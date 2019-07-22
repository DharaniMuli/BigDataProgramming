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
    val Dataframe1 = df.filter(df("category ") === "Music").orderBy(df("currency ")).show()
    //    val Dataframe2 = df.filter(df("state  ") === "successful").show()
    val upper: String => String = _.toUpperCase

    val upperUDF = udf(upper)
    df.withColumn("category ", upperUDF()).show()
    //  val ksSchema = StructType(
    //    List(
    //      StructField("ID",IntegerType,true),
    //      StructField("name",StringType,true)
    //
    //    )
    //  )
    //  val df = sc.createDataFrame(
    //    spark.sparkContext.parallelize(data),
    //    schema
    //  )

  }
}