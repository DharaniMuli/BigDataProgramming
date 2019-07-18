
import org.apache.spark._
import org.apache.spark.sql.types.{ArrayType, BooleanType, DataType, IntegerType, LongType, StringType, StructField, StructType}


import org.apache.spark.sql.SparkSession

object sparkdataframes {
  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val sparkconf = new SparkConf().setAppName("sparkdataframes").setMaster("local[*]")
    // Create a Scala Spark Context.
    val sc = new SparkContext(sparkconf)

    val input = sc.textFile("ksdata.csv")

//    input.foreach(println)

//    val ksSchema = StructType(
//        List(
//          StructField("ID",IntegerType),
//          StructField("name",StringType),
//          StructField("Category", ArrayType(StringType)),
//          StructField("main_Category",ArrayType(StringType)),
//          StructField("goal", LongType),
//          StructField("state",BooleanType),
//          StructField("currency", StructType(
//            List(
//              StructField("country", StringType),
//              StructField("pledged",IntegerType),
//              StructField("usdpledged", IntegerType)
//            )
//          ))
//
//        )
//      )
//    )

  }
}