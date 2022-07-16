import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import java.util.Properties

object SparkConnectMySQL {
  def main(args: Array[String]): Unit = {
    //Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    //Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder()
      .appName("MySQL-Connector")
      .master("local[*]")
      .getOrCreate()

    val user = "root"
    val password = "simple"
    // Config and get port from docker-compose.yml
        val database = "test_db"
        val table = "Transactions"
    val conString = s"jdbc:mysql://localhost:3307/$database"

    val connectionProperties = new Properties()
    connectionProperties.put("user", user)
    connectionProperties.put("password", password)

    import spark.implicits._
    // Read data source from "data.csv"
    val trans_df = spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("encoding", "ISO-8859-1")
      .csv("data/data.csv")

    // Load trans dataframe into table "Transactions" in test_db database
    trans_df.write.mode(SaveMode.Append).jdbc(conString, table, connectionProperties)

      // Read data from "Transactions" table
      val transactions = spark
        .read
        .format("jdbc")
        .option("driver", "com.mysql.jdbc.Driver")
        .option("url", conString)
        .option("dbtable", table)
        .option("user", user)
        .option("password", password)
        .load()

      // Calculating the total amount by Country each day
      val summary_df = transactions.withColumn("Date", split(col("InvoiceDate"), " ")(0))
        .groupBy("Country","Date")
        .agg(sum("UnitPrice").as("Total"))
        .cache()
      // Write Summary table into db

      // Calculating the first transaction of each user
      summary_df.write.jdbc(conString, "Summary", connectionProperties)
      val first_user_transactions = transactions.filter(col("CustomerID").isNotNull)
        .groupBy("CustomerID", "InvoiceNo")
        .agg(min(col("InvoiceDate")).as("Date")).distinct()
        .orderBy("Date")
        .cache()
      // Write FirstUserTransactions table into db
      first_user_transactions.write.jdbc(conString, "FirstUserTransactions", connectionProperties)
    spark.stop()
  }
}
