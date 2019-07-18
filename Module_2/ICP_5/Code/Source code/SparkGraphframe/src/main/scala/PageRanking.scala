import org.apache.spark.sql.{DataFrame, SparkSession}
import org.graphframes.GraphFrame


object PageRanking {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = SparkSession
      .builder()
      .appName("PageRanking")
      .config("spark.master", "local")
      .getOrCreate()

    val input= spark.read.format("csv").option("header","true").load(".\\src\\main\\scala\\tripdata.csv")

    // drop duplicates
    val df = input.dropDuplicates(input.columns)

    //    Output dataframes
//        df.show()

    //create vertices
    val vertices= df.select("Start Terminal","Start Station").
      withColumnRenamed("Start Terminal","id")
      .withColumnRenamed("Start Station","station")

    //Naming columns
    val edges = df.select("Start Terminal","End Terminal", "Duration")
      .withColumnRenamed("Start Terminal","src")
      .withColumnRenamed("End Terminal","dst")

    val g=GraphFrame(vertices,edges)

//        Show vertices
        g.vertices.show()
        //Show edges
        g.edges.show()
//
//    Finding TriangularCount
//    val results= g.triangleCount.run()
//    results.select("id", "count").show()

//    BFS
    val paths: DataFrame = g.bfs.fromExpr("id = '50'").toExpr("id='47'").run()

    val paths1: DataFrame = g.bfs.fromExpr("id = '50'").toExpr("id='47'").maxPathLength(2).run()
    paths.show()
    paths1.show()
//
////    Page Ranking
//
//    val results =g.pageRank.resetProbability(0.15).tol(0.01).run()
//
//    val results1 =g.pageRank.resetProbability(0.15).maxIter(5).run()
//
//    val results2 = g.pageRank.resetProbability(0.15).maxIter(10).sourceId("50").run()
//    results1.vertices.select("id","PageRank").show()
//    results1.edges.select("src","dst","Weight").show()
//
//    // Saving
//
//    g.vertices.write.format("csv").save("Vertices2")
//
//    g.edges.write.format("csv").save("Edges2")
  }
}
