
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

//Find Superheroes with re most obscure
object MostObscureSuperheroes {
  case class SupperHeroNames(id: Int, name: String)
  case class Connections(id: Int, connections: Int)

  def main(args: Array[String]): Unit = {
    //Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    //Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder()
      .appName("MostObscureSuperhero")
      .master("local[*]")
      .getOrCreate()


    //Create schema when reading Marvel-names.txt
    val supperHeroNamesSchema = new StructType()
      .add("Id", IntegerType, nullable = true)
      .add("Name", StringType, nullable = true)


    //Build up a hero ia, name dataset
    import spark.implicits._
    val supperHeroes = spark
      .read
      .schema(supperHeroNamesSchema)
      .option("sep", " ")
      .csv("data/Marvel-names.txt")
      .as[SupperHeroNames]

    supperHeroes.printSchema()
    supperHeroes.show()

    val connections = spark.sparkContext
      .textFile("data/Marvel-graph.txt")
      .map(line => {
        val id = line.split(" ").head.toInt
        val numberOfConnections = line.split(" ").tail.length
        (id, numberOfConnections)
      })
      .toDF("Id", "Connections")
      .as[Connections]

    //filter the connections to find rows with only one connection
    val one_connections = connections.filter(col("Connections") === 1)

    //Join two dataset to find result
    val result = supperHeroes.join(one_connections, supperHeroes("id") === one_connections("id"))

    result.select("*").show()

    print("The most obscure superhero is: " + result.count())
    spark.stop()
  }
}
