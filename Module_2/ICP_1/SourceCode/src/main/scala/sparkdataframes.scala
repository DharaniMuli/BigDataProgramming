import org.apache.avro.generic.GenericData.StringType
import org.apache.spark._
import org.apache.spark.sql.types.{StructField, StructType}



object sparkdataframes {
  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val sparkconf = new SparkConf().setAppName("sparkdataframes").setMaster("local[*]")
    // Create a Scala Spark Context.
    val sc = new SparkContext(sparkconf)

    val df = sc.textFile("ksdata.csv")

    df.foreach(println)
    //
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