
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructType}
import org.apache.log4j._
import org.apache.spark.sql.functions.{col, collect_list, count, countDistinct, date_trunc, explode, from_json, min, round, split, sum, to_date}
object KafkaNobelPrize {
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("Nobel-Prize")
      .master("local[*]")
      .getOrCreate()

    // Consume messages from the topic NOBEL-PRIZE kafka
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "NOBEL-PRIZE")
      .option("startingOffsets", "earliest")
      .load()

    df.printSchema()

    val schema = new StructType()
      .add("year", StringType)
      .add("category", StringType)
      .add("laureates", ArrayType(new StructType()
        .add("id", StringType)
        .add("firstname", StringType)
        .add("surname", StringType)
        .add("motivation", StringType)
        .add("share", StringType)
      ))

    // Select fields of the messages
    val json_DF = df.selectExpr("CAST(value as STRING)")
    val nobel_prize_DF =  json_DF.select(from_json(col("value"),schema).as("data"))
      .select(  "data.*")
      .withColumn("laureates", explode(col("laureates")))
      .select(
        col("laureates.id").as("id"),
        col("laureates.firstname").as("firstname"),
        col("laureates.surname").as("surname"),
        col("laureates.motivation").as("motivation"),
        col("laureates.share").as("share")
      )

    // Print the result each batch
    nobel_prize_DF
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }
}
