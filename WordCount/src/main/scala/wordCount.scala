import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object WordCount {
    def main(args: Array[String]) {
        val conf = new SparkConf().setMaster("local").setAppName("Word Count")
        val sc = new SparkContext(conf)
        val input = sc.textFile("/home/starbucks/spark-1.6.1-bin-hadoop2.6/README.md")
        val words = input.flatMap(line => line.split(" "))
        val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
        println("Word Count Scala Project")
        counts.saveAsTextFile("/home/starbucks/spark-1.6.1-bin-hadoop2.6/projects/WordCount/outputs/wordcount")
    }
}
