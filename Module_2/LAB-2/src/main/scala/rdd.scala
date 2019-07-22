
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf



object rdd {
def main(args: Array[String]):Unit = {
  val conf = new SparkConf().setAppName("rdd").setMaster("local")
  val sc= new SparkContext(conf)

//    val spark = SparkSession.builder.appName("Analysis").master("local").getOrCreate()
      val dataRDD = sc.textFile("D:\\Semester-2\\BigDataPrgramming\\Module_2\\LAB-2\\ksdata.csv")

      // Get 10 results
//      dataRDD.take(10).foreach(println)

      // Getting length of each line from the top 10 results
    val linelenghts = dataRDD.take(10).map(len => len.length)
//    linelenghts.foreach(println)

  val firstelements = dataRDD.first()
//  println(firstelements)

// Getting total length of top 10 results
    val totallength = linelenghts.reduce((a,b) =>a+b)
//    println(totallength)

  // Function Call for Query 1

  val ouput = dataRDD.filter(MyFunctions.func1).take(5)


//  val categorymap = dataRDD.map(MyFunctions.counter)
//  val results  = categorymap.reduceByKey(_ +_)
//  results.foreach(println)
}
}
// Query1: Function Which returns all the rows whose Category poetry
object MyFunctions{
  def func1(s:String):Boolean = {
    s.split(",")(2) == "Poetry"
  }

  def counter(s:String): (String, Int) = {
    (s.split(",")(4),1)
  }
}