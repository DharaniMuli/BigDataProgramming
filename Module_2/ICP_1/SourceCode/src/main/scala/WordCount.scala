/**
  * Illustrates flatMap + countByValue for wordcount.
  */


import org.apache.spark._

object WordCount {
  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C:\\winutils" )
    //val inputFile = args(0)
    //val outputFile = args(1)
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[*]")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    // Load our input data.
    //val input =  sc.textFile(inputFile)
    val input = sc.textFile("input.txt")
    // Split up into words.

//    Bonus code starts
//    val words = input.flatMap(line => line.split(" "))
//    //.filter(value => value=="good")
//    val wCount = words.filter(word => Character.isLetter(word.head)) // ignores numbers
//      .map(word => (word.head, 1)) // gets the first letter of each word
//    val result = wCount.reduceByKey((x, y) => x + y)
//    result.collect().foreach(println)
//    Bonus code ends

 val words = input.flatMap(line => line.split(" "))
//   //.filter(value => value=="hello") // code for filtering
//    // Transform into word and count.
val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
.sortBy(_._1, ascending = true) // code for sorting
println(counts.take(4).foreach(println))
//
//    val result = words.distinct()
//    println(result.collect().mkString(","))
//    println("Word Count",words.count())
//    // Save the word count back out to a text file, causing evaluation.
    counts.saveAsTextFile("WordCountOutputtake")
  }
}
