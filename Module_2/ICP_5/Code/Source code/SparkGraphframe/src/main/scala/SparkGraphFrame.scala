import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame
import org.graphframes._

import org.apache.spark.graphx._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
object SparkGraphFrame {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()

    val input= spark.read.format("csv").option("header","true").load
    ("D:\\Semester-2\\BigDataPrgramming\\Module_2\\ICP_5\\Code\\Source code\\SparkGraphframe\\src\\main\\scala\\tripdata.csv")


    input.registerTempTable("df")
    spark.sql("SELECT CONCAT(k, ' ',  v) FROM df")


    // drop duplicates
    val df = input.dropDuplicates(input.columns)
//
////    Output dataframes
//    df.show()


    //create vertices
    val vertices= df.select("Start Terminal","Start Station").
      withColumnRenamed("Start Terminal","id")
      .withColumnRenamed("Start Station","station")

    //Naming columns
    val edges = df.select("Start Terminal","End Terminal", "Duration")
      .withColumnRenamed("Start Terminal","src")
      .withColumnRenamed("End Terminal","dst")



    val g = GraphFrame(vertices,edges)

//    Show vertices
//    g.vertices.show()
//    //Show edges
//    g.edges.show()


 val motifs = g.find("(50)-[e]->(70); (70)-[e2]->(50)").show()


    val inDeg = g.inDegrees
    inDeg.orderBy(desc("inDegree")).limit(5).show()


    val outDeg = g.outDegrees
    outDeg.orderBy(desc("outDegree")).limit(5).show()

    //Bonus
    val degreeRatio = inDeg.join(outDeg, inDeg.col("id") === outDeg.col("id"))
      .drop(outDeg.col("id"))
      .selectExpr("id", "double(inDegree)/double(outDegree) as degreeRatio")

    degreeRatio.cache()


    degreeRatio.orderBy(desc("degreeRatio")).limit(10).show()

    g.degrees.show()

    val commonTrips = g
      .edges
      .groupBy("src", "dst")
      .count()
      .orderBy(desc("count"))
      .limit(10)
      commonTrips.show()


//    df.select("name", "favorite_color").write.save("sample.csv")

  }
}
