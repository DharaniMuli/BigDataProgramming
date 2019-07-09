import org.apache.spark.sql.SparkSession

object bonus {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()




    def splitter =(str: String) => {
      str.split(",")
    }
    spark.udf.register("split",splitter)
    val df=splitter("Hi,how,are,you")
    df.foreach(println(_))

  }
}
