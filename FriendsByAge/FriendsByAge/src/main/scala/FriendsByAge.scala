import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

// Calculating average friends by age in a social network
object FriendsByAge {

  def parseLine(line: String): (Int, Int) = {
    val fields = line.split(",")
    val age = fields(2).toInt
    val numberOfFriends = fields(3).toInt
    (age, numberOfFriends)
  }

  def main(args: Array[String]): Unit = {
    //Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    //Create a SparkContext using very core in the local machine
    val sc = new SparkContext("local[*]", "FriendsByAge")

    //Load each line of the source data into an RDD
    val lines = sc.textFile("data/fakefriends-noheader.csv")

    //Using parseLine function to convert to (age, numberOfFriends)
    val rdd = lines.map(parseLine)

    // Starting with pair RDD[age, numberOfFriends] where age is key and numberOfFriends is value
    // mapValues to convert each numberOfFriends value to a tuple of (numberOfFriends, 1)
    // reduceByKey to sum numberOfFriends and total instance by each age
    val totalByAge = rdd.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1,x._2+ y._2))

    // mapValues to convert each tuple using first value division second value
    val result = totalByAge.mapValues(x => x._1/x._2)

    // display average fiends by age
    result.foreach(println)
  }
}
