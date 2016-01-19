import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object PopularRoutesApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Calculates the popular routes")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile("Car_Routes.txt")
    val routeMap = textFile.flatMap(line => line.split(",")).map(word => (word, 1)).reduceByKey((a,b) => a+b)
    	println("Car_Routes")
	routeMap foreach {case (key, value) => println (key + "-->" + value)}
  }
}