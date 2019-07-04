import org.apache.spark.{SparkConf, SparkContext}

object MergeSort {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "c:\\winutils")
    val conf = new SparkConf().setAppName("mergesort").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val list = List(List(38,27,43,3,9,82,10))
    val b = sc.parallelize(list)


    def mergeSort(xs: List[Int]): List[Int] = {
      val n = xs.length / 2
      if (n == 0) xs
      else {
        def merge(xs: List[Int], ys: List[Int]): List[Int] =
          (xs, ys) match {
            case(Nil, ys) => ys
            case(xs, Nil) => xs
            case(x :: xs1, y :: ys1) =>
              println(x,y)
              if (x < y) x::merge(xs1, ys)
              else y :: merge(xs, ys1)
          }
        val (left, right) = xs splitAt(n)
        merge(mergeSort(left), mergeSort(right))
      }
    }
    val result= b.map(mergeSort)
    print(result)

  }
}
