import org.apache.spark.sql.{DataFrame, SparkSession}
import org.graphframes.GraphFrame


object GraphFrames {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = SparkSession
      .builder()
      .appName("PageRanking")
      .config("spark.master", "local")
      .getOrCreate()

    val input= spark.read.format("csv").option("header","true").load(".\\Datasets\\group-edges.csv")

    // drop duplicates
    val df = input.dropDuplicates(input.columns)

//        Output dataframes
//            df.show()

    //create vertices
    val vertices= df.select("group1","weight").
      withColumnRenamed("group1","id")
      .withColumnRenamed("weight","weight")

    //Naming columns
    val edges = df.select("group1","group2", "weight")
      .withColumnRenamed("group1","src")
      .withColumnRenamed("group2","dst")

    val g=GraphFrame(vertices,edges)
    //    g.vertices.show()
    //    g.edges.show()


    //    Page Ranking Algorithm


    val result4 = g.pageRank.resetProbability(0.15).maxIter(3).run()
    result4.vertices.select("id", "pagerank").show(10)
    result4.edges.select("src", "dst", "weight").distinct().show(10)



  }
}
